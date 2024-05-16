from dotenv import load_dotenv
import os
from langchain_community.embeddings import BedrockEmbeddings
from langchain_community.vectorstores import OpenSearchVectorSearch
import chainlit as cl
from langchain_community.embeddings import BedrockEmbeddings
from langchain_community.vectorstores import OpenSearchVectorSearch
from opensearchpy import AWSV4SignerAuth
import boto3
from opensearchpy import RequestsHttpConnection
from langchain.chains.query_constructor.base import AttributeInfo
from langchain.retrievers.self_query.base import SelfQueryRetriever
from langchain_aws import ChatBedrock
from langchain_core.messages import HumanMessage
from langchain.chains.retrieval import create_retrieval_chain
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain import hub
import random

# load the environment variables
load_dotenv()
os.environ["AWS_PROFILE"] = 'currikiai'

if os.getenv("AWS_OPENSEARCH_DOMAIN_ENDPOINT") is None:
    print("Please set the environment variables. Program will exit now.")
    exit()


@cl.on_chat_start
async def on_chat_start():
    aws_opensearch_url = os.getenv("AWS_OPENSEARCH_DOMAIN_ENDPOINT")
    credentials = boto3.Session().get_credentials()
    region = 'us-west-2'
    awsauth = AWSV4SignerAuth(credentials, region)

    bedrock_embeddings = BedrockEmbeddings(credentials_profile_name=os.environ["AWS_PROFILE"], region_name=region)
    vectorstore = OpenSearchVectorSearch(opensearch_url=aws_opensearch_url, 
        index_name='curriki-library-index', 
        embedding_function=bedrock_embeddings,
        http_auth=awsauth,
        timeout=300,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
    )

    # user_query = "What is money supply in Social Studies?"
    # docs = vectorstore.similarity_search(user_query, top_k=5)

    metadata_field_info = [
        AttributeInfo(
            name="title",
            description="Title of the Open Education Resource (OER) in the Curriki Library",
            type="string",
        ),
        AttributeInfo(
            name="pageurl",
            description="URL of the Open Education Resource (OER) page in the Curriki Library",
            type="string",
        ),
        AttributeInfo(
            name="description",
            description="Description of the Open Education Resource (OER) in the Curriki Library",
            type="string",
        ),
        AttributeInfo(
            name="content",
            description="Additional information of the Open Education Resource (OER) in the Curriki Library",
            type="string",
        ),
        AttributeInfo(
            name="keywords",
            description="Keywords which can be used to search and associate the Open Education Resource (OER) in the Curriki Library",
            type="string",
        ),
        AttributeInfo(
            name="educationlevels",
            description="Education levels of the Open Education Resource (OER) in the Curriki Library",
            type="string",
        ),
        AttributeInfo(
            name="subjectareas",
            description="Subject areas of the Open Education Resource (OER) in the Curriki Library",
            type="string",
        ),
        AttributeInfo(
            name="parentcollections",
            description="Parent collections of the Open Education Resource (OER) in the Curriki Library",
            type="string",
        )
    ]

    document_content_description = "The content from one or more Open Education Resources (OERs) in the Curriki Library"

    llm = ChatBedrock(
        model_id="anthropic.claude-v2",
        model_kwargs={"temperature": 0.1},
    )
    query_retriever = SelfQueryRetriever.from_llm(
        llm=llm,
        vectorstore=vectorstore,
        metadata_field_info=metadata_field_info,
        document_contents=document_content_description,
        return_source_documents=True
    )
    
    qa_chat_prompt = hub.pull("langchain-ai/retrieval-qa-chat")
    chain = create_retrieval_chain(
        retriever=query_retriever,
        combine_docs_chain=create_stuff_documents_chain(llm, prompt=qa_chat_prompt)
    )
    cl.user_session.set("chain", chain)
    cl.user_session.set("retriever", query_retriever)
    
@cl.on_message
async def on_message(message):

    #query_retriever = cl.user_session.get("retriever")
    #docs = query_retriever.invoke("20th century economist who might mentioned in education level of Graduate,")
    chain = cl.user_session.get("chain")
    response = chain.invoke({"input": message.content})
    answer = HumanMessage(content=response['answer']).content
    oer_references = []
    # iterate over the context
    for index, context in enumerate(response['context']):
        oer_title = context.metadata['title']
        oer_reference_text = f"Title: {oer_title}\n\nURL: {context.metadata['pageurl']}\n\nSource File: {context.metadata['source']}\n\nPage:{context.metadata['page']}\n\n"
        oer_reference_text += f"\n\nEducation Levels: {context.metadata['educationlevels']}\n\nSubject Areas: {context.metadata['subjectareas']}\n\nParent Collections: {context.metadata['parentcollections']}"
        oer_reference_text += f"\n\nContent{context.page_content}"
        oer_references.append(
            cl.Text(content=oer_reference_text, name=f"{oer_title} ({index})")
        )
    oer_reference_names = [oer_reference.name for oer_reference in oer_references]
    
    if oer_reference_names:
        answer += f"\n\nSources: {', '.join(oer_reference_names)}"

    await cl.Message(content=answer, elements=oer_references).send()