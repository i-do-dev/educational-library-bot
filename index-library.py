import mysql.connector
from dotenv import load_dotenv
import os
import pandas as pd
from langchain_community.document_loaders import PyPDFLoader
import boto3
import boto3.session
from langchain.text_splitter import RecursiveCharacterTextSplitter
from bs4 import BeautifulSoup
from langchain_community.embeddings import BedrockEmbeddings
from langchain_community.vectorstores import OpenSearchVectorSearch
from opensearchpy import AWSV4SignerAuth
from opensearchpy import RequestsHttpConnection

# load the environment variables
load_dotenv()

if os.getenv("AWS_OPENSEARCH_DOMAIN_ENDPOINT") is None:
    print("Please set the environment variables. Program will exit now.")
    exit()

#==========================================================================
#====================== PREPARE DATA FOR EMBEDDINGS =======================
#==========================================================================

# connect to the database
cnx = mysql.connector.connect(user=os.getenv('MYSQL_USER'), password=os.getenv('MYSQL_PASSWORD'),
                              host=os.getenv('MYSQL_HOST'),
                              database=os.getenv('MYSQL_DATABASE'))
cursor = cnx.cursor()

query_offset = 0
query_limit = 1

# query to get the resourcefiles
resourcefiles_query = (f"""
    SELECT 
        rf.fileid, rf.resourceid, rf.filename, rf.ext, rf.s3path,
        r.title, r.pageurl, r.active, r.description, r.content, r.keywords
    FROM 
        resourcefiles rf
    LEFT JOIN 
        resources r ON rf.resourceid = r.resourceid
    WHERE 
        r.type = 'resource' AND r.active = 'T' AND rf.ext = 'pdf' AND s3path IS NOT NULL AND s3path <> '' 
    ORDER BY rf.fileid
    LIMIT {query_limit} OFFSET {query_offset};
""")

# Read the query result into a pandas DataFrame
resourcefiles_df = pd.read_sql(resourcefiles_query, cnx)

# create empty lists to store the education levels, subject areas and parent collections
edu_levels_list = []
subject_areas_list = []
parent_collections_list = []

# Iterate over the resourcefiles dataframe
for index, row in resourcefiles_df.iterrows():
    resourceid = row['resourceid']
    edu_level_query = ("SELECT el.`resourceid`, e.`levelid`, e.`displayname` FROM `resource_educationlevels` AS el LEFT JOIN `educationlevels` AS e ON (el.`educationlevelid` = e.`levelid`) WHERE el.`resourceid` = " + str(resourceid) + ";")
    edu_level_df = pd.read_sql(edu_level_query, cnx)
    # append the education levels to the list with resourceid and displayname as comma separated string
    edu_levels_list.append( pd.DataFrame( {'resourceid': [resourceid], 'educationlevels': [', '.join(edu_level_df['displayname'])]}) )

    sub_query = ("SELECT CONCAT(s.displayname, \", \", sa.displayname) AS displayname, sa.subjectareaid FROM `resource_subjectareas` AS rs LEFT JOIN `subjectareas` AS sa ON (rs.`subjectareaid` = sa.`subjectareaid`) inner join subjects s on sa.subjectid = s.subjectid WHERE rs.`resourceid` = " + str(resourceid) + ";")
    sub_df = pd.read_sql(sub_query, cnx)
    # append the subject areas to the list with resourceid and displayname as comma separated string
    subject_areas_list.append( pd.DataFrame( {'resourceid': [resourceid], 'subjectareas': [', '.join(sub_df['displayname'])]}) )

    collection_query = ("SELECT GROUP_CONCAT(c.title SEPARATOR ', ') AS parentcollections FROM resources r JOIN collectionelements ce ON r.resourceid = ce.resourceid JOIN resources c ON ce.collectionid = c.resourceid WHERE r.type = 'resource' AND r.resourceid = " + str(resourceid) + ";")
    collection_df = pd.read_sql(collection_query, cnx)
    # append the parent collections to the list with resourceid and parentcollections as comma separated string
    parent_collections_list.append( pd.DataFrame( {'resourceid': [resourceid], 'parentcollections': [', '.join(filter(None, collection_df['parentcollections']))] }) )

# create dataframes from the lists
edu_levels_df = pd.concat(edu_levels_list)
subject_areas_df = pd.concat(subject_areas_list)
parent_collections_df = pd.concat(parent_collections_list)

# merge the dataframes with the resourcefiles dataframe
resourcefiles_df = pd.merge(resourcefiles_df, edu_levels_df, on='resourceid', how='left')
resourcefiles_df = pd.merge(resourcefiles_df, subject_areas_df, on='resourceid', how='left')
resourcefiles_df = pd.merge(resourcefiles_df, parent_collections_df, on='resourceid', how='left')

#==========================================================================
#=========================== SETUP EMMBEDDINGS ============================
#==========================================================================
os.environ["AWS_PROFILE"] = 'currikiai'
aws_opensearch_url = os.getenv("AWS_OPENSEARCH_DOMAIN_ENDPOINT")
credentials = boto3.Session().get_credentials()
region = 'us-west-2'
awsauth = AWSV4SignerAuth(credentials, region)
s3 = boto3.client('s3')

# iterate over the resourcefiles dataframe to extract the text from the pdf files
for index, row in resourcefiles_df.iterrows():
    file_name = row['filename']
    s3_path = row['s3path']
    
    s3_bucket = s3_path.split('/')[2].split('.')[0]
    resourcefile_s3_download_path = '/'.join(s3_path.split('/')[3:])
    resourcefile_s3_name = resourcefile_s3_download_path.split('/')[-1]

    # download resourcefile_s3_download_path from s3_bucket
    s3.download_file(s3_bucket, resourcefile_s3_download_path, resourcefile_s3_name)

    fileLoader = None
    # load the pdf file if row['ext'] is 'pdf'
    if row['ext'] == 'pdf':
        fileLoader = PyPDFLoader(resourcefile_s3_name)
    
    if fileLoader is not None:
        loaded_document = fileLoader.load()
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=0)
        docs = text_splitter.split_documents(loaded_document)

        content_soup = BeautifulSoup(row['content'], 'html.parser')
        content_text = content_soup.get_text()
        # remove newlines and extra spaces
        content_text = ' '.join(content_text.split())

        # description is in html format, so convert it to text
        description_soup = BeautifulSoup(row['description'], 'html.parser')
        description_text = description_soup.get_text()
        # remove newlines and extra spaces
        description_text = ' '.join(description_text.split())

        # join row['pageurl'] with base url https://www.currikilibrary.org/
        pageurl = 'https://www.currikilibrary.org/oer/' + row['pageurl']

        # iterate over the documents and add metadata to each document all columns of the resourcefiles_df
        for i, doc in enumerate(docs):
            doc.metadata['title'] = row['title']
            doc.metadata['pageurl'] = pageurl
            doc.metadata['description'] = description_text
            doc.metadata['content'] = content_text
            doc.metadata['keywords'] = row['keywords']
            doc.metadata['educationlevels'] = row['educationlevels']
            doc.metadata['subjectareas'] = row['subjectareas']
            doc.metadata['parentcollections'] = row['parentcollections']
            
        # split the documents array into chunks of 500
        bulk_size = 500
        bulk_docs = [docs[i:i + bulk_size] for i in range(0, len(docs), bulk_size)]

        for docs_chunk in bulk_docs:    
            # get the embeddings
            bedrock_client = boto3.client(service_name='bedrock-runtime', region_name='us-west-2')
            embeddings = BedrockEmbeddings(model_id="amazon.titan-embed-text-v1", client=bedrock_client)
            vectorstore = OpenSearchVectorSearch.from_documents(
                docs_chunk,
                embeddings,
                opensearch_url=aws_opensearch_url,
                http_auth=awsauth,
                timeout=300,
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection,
                index_name="curriki-library-index"
            )

        print(docs)
        print(f"*** {len(docs)} documents created for {resourcefile_s3_name} ***")
       

cursor.close()
cnx.close()
