import boto3.session
from langchain_community.embeddings import BedrockEmbeddings
from langchain_community.document_loaders import TextLoader, PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from pymongo import MongoClient
from langchain_community.vectorstores.documentdb import DocumentDBVectorSearch
from langchain_community.vectorstores.documentdb import DocumentDBSimilarityType
from langchain_chroma.vectorstores import Chroma
from langchain_community.vectorstores import OpenSearchVectorSearch
from opensearchpy import RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
from dotenv import load_dotenv
import os
import sqlite3
import datetime

# load environment variables
load_dotenv()
VDB_USERNAME = os.getenv('VDB_USERNAME')
VDB_PASSWORD = os.getenv('VDB_PASSWORD')
VDB_CLUSTER_ENDPOINT = os.getenv('VDB_CLUSTER_ENDPOINT')
VDB_CLUSTER_PORT = os.getenv('VDB_CLUSTER_PORT')
VDB_INDEX_NAME = "currikiai-index"
VDB_NAMESPACE = "currikiai_db.currikidocs_collection"
VDB_DB_NAME, VDB_COLLECTION_NAME = VDB_NAMESPACE.split(".")

# check if environment variables are set
if VDB_USERNAME is None or VDB_PASSWORD is None or VDB_CLUSTER_ENDPOINT is None or VDB_CLUSTER_PORT is None:
    print("Environment variables not set")
    exit()

# DocumentDB connection string
# i.e., "mongodb://{username}:{pass}@{cluster_endpoint}:{port}/?{params}"
#VDB_CONNECTION_STRING = f"mongodb://{VDB_USERNAME}:{VDB_PASSWORD}@{VDB_CLUSTER_ENDPOINT}:{VDB_CLUSTER_PORT}/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
#vdb_client: MongoClient = MongoClient(VDB_CONNECTION_STRING)
#vdb_collection = vdb_client[VDB_DB_NAME][VDB_COLLECTION_NAME]

db_connection = sqlite3.connect('curriki.db')
# create table 'processed_files' if it does not exist
db_connection.execute('CREATE TABLE IF NOT EXISTS processed_files (file_name TEXT PRIMARY KEY, processed BOOLEAN)')
db_connection.commit()

# clear the processed_files table
#db_connection.execute('DELETE FROM processed_files')
#db_connection.commit()
#exit()

os.environ["AWS_PROFILE"] = 'currikiai'

# boto3 get token
""" session = boto3.session.Session()
sts_client = session.client('sts')
aws_credentials = sts_client.get_session_token() """
""" print(aws_credentials['Credentials']['SecretAccessKey'])
print(aws_credentials['Credentials']['AccessKeyId'])
print(aws_credentials['Credentials']['SessionToken'])
 """
#awsauth = AWS4Auth(aws_credentials['Credentials']['AccessKeyId'], aws_credentials['Credentials']['SecretAccessKey'], 'us-east-1', 'aoss', aws_credentials['Credentials']['SessionToken'])


# list s3 bucket 'curriki-knowledge-bucket' files
s3 = boto3.client('s3')
response = s3.list_objects_v2(Bucket='curriki-knowledge-bucket')
files = response['Contents']

# get the list of files that have not been processed
cursor = db_connection.cursor()
cursor.execute('SELECT file_name FROM processed_files WHERE processed = 1')
processed_files = cursor.fetchall()
processed_files = [file[0] for file in processed_files]
# process the files
for file in files:
    if file['Key'] not in processed_files:
        original_file_name = file['Key']
        
        # process the file
        # get the file from s3
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        file_name = file['Key'].split('/')[-1]
        new_file_name = f"{timestamp}_{file_name}"
        s3.download_file('curriki-knowledge-bucket', file['Key'], new_file_name)
        print(f"*** Processing {original_file_name} ***")
        # fileLoader define as null
        fileLoader = None
        # check file type and process accordingly
        if file_name.endswith('.txt'):
            fileLoader = TextLoader(new_file_name)
        elif file_name.endswith('.pdf'):
            fileLoader = PyPDFLoader(new_file_name)

        if fileLoader is not None:
            documents = fileLoader.load()
            text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
            docs = text_splitter.split_documents(documents)
            print(f"*** {len(docs)} documents created for {original_file_name} ***")
             
            for i, doc in enumerate(docs):
                doc.metadata['source'] = f"source_{i+1}_{original_file_name}"
            
            # get the embeddings
            bedrock_client = boto3.client(service_name='bedrock-runtime', region_name='us-east-1')
            bedrock_embeddings = BedrockEmbeddings(model_id="amazon.titan-embed-text-v1", client=bedrock_client)
            #[print(f"*** {docs[i].page_content}") for i in range(len(docs))]
            """ vectorstore = DocumentDBVectorSearch.from_documents(
                documents=docs,
                embedding=bedrock_embeddings,
                collection=vdb_collection,
                index_name=VDB_INDEX_NAME,
            )
            vectorstore.create_index() """
            vectorstore = Chroma.from_documents(
                documents=docs,
                embedding=bedrock_embeddings,
                collection_name='currikidocs_collection',
                persist_directory="./chroma_db"
            )
            print(f"*** Vectorstore created for {original_file_name} ***")
            """ query = "Curriki"
            searched_docs = vectorstore.similarity_search_with_score(query)
            print(f">>> {searched_docs}") """
        
        # insert file_name into processed_files
        db_connection.execute('INSERT INTO processed_files (file_name, processed) VALUES (?, ?)', (original_file_name, 1))
        db_connection.commit()
        # remove the file
        os.remove(new_file_name)

# close the cursor
cursor.close()

# close the connection
db_connection.close()