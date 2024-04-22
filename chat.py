import boto3.session
import chainlit as cl
from langchain_chroma.vectorstores import Chroma
import boto3
from langchain_community.embeddings import BedrockEmbeddings
import os
from langchain.chains.qa_with_sources.retrieval import RetrievalQAWithSourcesChain 
from langchain_aws import ChatBedrock
from langchain_core.messages import HumanMessage

os.environ["AWS_PROFILE"] = 'currikiai'

@cl.on_chat_start
async def on_message():
    #os.environ["AWS_PROFILE"] = 'currikiai'
    bedrock_embeddings = BedrockEmbeddings(credentials_profile_name='currikiai', region_name='us-east-1')
    chroma = Chroma(persist_directory="./chroma_db", collection_name='currikidocs_collection', embedding_function=bedrock_embeddings)
    #print(f"data >>>> {chroma.get()}")

    chain = RetrievalQAWithSourcesChain.from_chain_type(llm=ChatBedrock(
        model_id="anthropic.claude-v2",
        model_kwargs={"temperature": 0.1},
    ),
    chain_type="stuff",
    retriever=chroma.as_retriever())
    cl.user_session.set('curriki_ai_chain', chain)

@cl.on_message
async def on_message(message: cl.Message):
    cb = cl.AsyncLangchainCallbackHandler(stream_final_answer=True, answer_prefix_tokens=["FINAL","ANSWER"])
    cb.answer_reached = True
    chain = cl.user_session.get('curriki_ai_chain')
    response = chain({'question': message.content, 'callback': [cb]})
    print(f">>> {response}")
    await cl.Message(HumanMessage(content=response['answer']).content).send()
    
