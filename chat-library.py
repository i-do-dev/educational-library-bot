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
            description="Education levels of the Open Education Resource (OER) in the Curriki Library. It can be one or more of the following: displayname, Elementary School, Grade 1, Grade 10, Grade 11, Grade 12, Grade 2, Grade 3, Grade 4, Grade 5, Grade 6, Grade 7, Grade 8, Grade 9, Graduate, Higher Education, HighSchool, K, Lifelong Learning, Middle School, Other, Pre-K, PreKto12, Professional Education & Development, Special Education, Undergraduate-Lower Division, Undergraduate-Upper Division, Vocational Training",
            type="string",
        ),
        AttributeInfo(
            name="subjectareas",
            description="Subject areas of the Open Education Resource (OER) in the Curriki Library. It can be one or more of the following: Accessibility, Special Education, Adult Education, Agriculture, Algebra, Alphabet, Anthropology, Applied Mathematics, Architecture, Arithmetic, Astronomy, Bilingual Education, Biology, Body Systems & Senses, Botany, Business, Calculus, Careers, Careers in CS, Chemistry, Civics, Classroom Management, Coding, Computational Thinking, Computer Graphics , Computers in Society, Computing and Data Analysis , Cultural Awareness, Current Events, Dance, Data Analysis & Probability, Drama/Dramatics, Early Childhood Education, Earth Science, Ecology, Economics, Education Administration/Leadership, Educational Foundations/Research, Educational Psychology, Engineering, Entrepreneurship, Environmental Health, Equations, Estimation, Evaluating Sources, Film, General, General Science, Geography, Geology, Geometry, Global Awareness, Government, Grammar, Usage & Awareness, Grammar, Usage & Mechanics, Graphing, History, History of Science, History/Local, Human Computer Interaction, Human Sexuality, Informal Education, Instructional Design, Integrating Technology into the Classroom, Journalism, Life Sciences, Linguistics, Listening & Speaking, Listening Comprehension, Literature, Measurement, Measurement & Evaluation, Media Ethics, Mental/Emotional Health, Mentoring, Meteorology, Multicultural Education, Music, Natural History, Number Sense & Operations, Nutrition, Occupational Home Economics, Oceanography, Online Safety, Paleontology, Patterns, Phonics, Photography, Physical Education, Physical Sciences, Physics, Poetry, Political Systems, Popular Culture, Privacy and Security, Problem Solving, Process Skills, Programming Languages, Psychology, Reading, Reading Comprehension, Religion, Research, Research Methods, Robotics, Safety/Smoking/Substance Abuse Prevention, School-to-Work, Sociology, Speaking, Spelling, StandardsAlignment, State History, Statistics, Story Telling, Teaching Techniques/Best Practices, Technology, Thinking & Problem Solving, Trade & Industrial, Trigonometry, United States Government, United States History, Using Multimedia & the Internet, Visual Arts, Vocabulary, Web Design, Web programming, World History, Writing",
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

    chain = SelfQueryRetriever.from_llm(
        llm=llm,
        vectorstore=vectorstore,
        metadata_field_info=metadata_field_info,
        document_content_description=document_content_description,
    )
    