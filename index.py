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
import sqlite3

# load the environment variables
load_dotenv()

if os.getenv("AWS_OPENSEARCH_DOMAIN_ENDPOINT") is None:
    print("Please set the environment variables. Program will exit now.")
    exit()

db_connection = sqlite3.connect('curriki.db')
cursor_lite = db_connection.cursor()
# create table 'processed_resourcefile' if it does not exist.
db_connection.execute('CREATE TABLE IF NOT EXISTS processed_resourcefile (fileid INTEGER PRIMARY KEY);')
db_connection.commit()

# count the number of rows in the table 'processed_resourcefile'. if the count is 0, insert the fileid into the table otherwise update the fileid.
cursor_lite.execute('SELECT COUNT(*) FROM processed_resourcefile;')
count = cursor_lite.fetchone()[0]
if count == 0:
    db_connection.execute('INSERT INTO processed_resourcefile (fileid) VALUES (?);', (0,))
db_connection.commit()

cursor_lite.execute('SELECT * FROM processed_resourcefile;')
processed_resourcefile_id = cursor_lite.fetchone()[0]

""" 
db_connection.execute('UPDATE processed_resourcefile SET fileid = ?;', (1451,))
db_connection.commit()
exit() """
#==========================================================================
#====================== PREPARE DATA FOR EMBEDDINGS =======================
#==========================================================================

# connect to the database
cnx = mysql.connector.connect(user=os.getenv('MYSQL_USER'), password=os.getenv('MYSQL_PASSWORD'),
                              host=os.getenv('MYSQL_HOST'),
                              database=os.getenv('MYSQL_DATABASE'))
cursor = cnx.cursor()

query_offset = 0
query_limit = 100

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
        r.type = 'resource' AND r.active = 'T' AND rf.ext = 'pdf' AND s3path IS NOT NULL AND s3path <> '' AND rf.fileid > {processed_resourcefile_id} 
    ORDER BY rf.fileid
    LIMIT {query_limit} OFFSET {query_offset};
""")

# Read the query result into a pandas DataFrame
resourcefiles_df = pd.read_sql(resourcefiles_query, cnx)

# create empty lists to store the education levels, subject areas and parent collections
edu_levels_list = []
subject_areas_list = []
collections_list = []

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

    collection_query = ("SELECT GROUP_CONCAT(c.title SEPARATOR ', ') AS collections FROM resources r JOIN collectionelements ce ON r.resourceid = ce.resourceid JOIN resources c ON ce.collectionid = c.resourceid WHERE r.type = 'resource' AND r.resourceid = " + str(resourceid) + ";")
    collection_df = pd.read_sql(collection_query, cnx)
    # append the parent collections to the list with resourceid and collections as comma separated string
    collections_list.append( pd.DataFrame( {'resourceid': [resourceid], 'collections': [', '.join(filter(None, collection_df['collections']))] }) )

# create dataframes from the lists
edu_levels_df = pd.concat(edu_levels_list)
subject_areas_df = pd.concat(subject_areas_list)
collections_df = pd.concat(collections_list)

# merge the dataframes with the resourcefiles dataframe
resourcefiles_df = pd.merge(resourcefiles_df, edu_levels_df, on='resourceid', how='left')
resourcefiles_df = pd.merge(resourcefiles_df, subject_areas_df, on='resourceid', how='left')
resourcefiles_df = pd.merge(resourcefiles_df, collections_df, on='resourceid', how='left')

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
    s3_path = row['s3path']
    
    s3_bucket = s3_path.split('/')[2].split('.')[0]
    resourcefile_s3_download_path = '/'.join(s3_path.split('/')[3:])
    resourcefile_s3_name = resourcefile_s3_download_path.split('/')[-1]

    try:
        # download resourcefile_s3_download_path from s3_bucket
        s3.download_file(s3_bucket, resourcefile_s3_download_path, resourcefile_s3_name)
    except Exception as e:
        continue
    # download resourcefile_s3_download_path from s3_bucket
    #s3.download_file(s3_bucket, resourcefile_s3_download_path, resourcefile_s3_name)

    fileLoader = None
    # load the pdf file if row['ext'] is 'pdf'
    if row['ext'] == 'pdf':
        fileLoader = PyPDFLoader(resourcefile_s3_name)
    
    if fileLoader is not None:
        try:
            loaded_document = fileLoader.load()
        except Exception as e:
            continue
        #loaded_document = fileLoader.load()
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
            if i == 0:
                doc.page_content = f"""
                Open Education Resource Title: {row['title']} 
                \n Open Education Resource Description: {description_text} 
                \n Open Education Resource Content: {content_text}
                \n {doc.page_content}
                """
            
            doc.metadata['title'] = row['title']
            doc.metadata['pageurl'] = pageurl
            doc.metadata['description'] = description_text
            doc.metadata['content'] = content_text
            # split row['keywords'] by space and join with comma and make string lowercase
            if row['keywords'] is not None:
                keywords = ', '.join(row['keywords'].split()).lower()
            else:
                keywords = ''
            doc.metadata['keywords'] = keywords
            doc.metadata['educationlevels'] = row['educationlevels']
            doc.metadata['subjectareas'] = row['subjectareas']
            doc.metadata['collections'] = row['collections']
            
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
                index_name="curriki-oer-library-index"
            )
            print(f"fileid:{row['fileid']} -- {len(docs_chunk)} chunks saved ....")
            print('-----------------------------------')
            
        print(f"*** fileid:{row['fileid']} -- {len(docs)} documents created for {row['title']} - {resourcefile_s3_name} ***")
        print('=========================================')
    
    # remove the downloaded file
    os.remove(resourcefile_s3_name)
    db_connection.execute('UPDATE processed_resourcefile SET fileid = ?;', (row['fileid'],))
    db_connection.commit()
    cursor_lite.close()
    

cursor.close()
cnx.close()
