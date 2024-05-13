""" SELECT GROUP_CONCAT(c.title SEPARATOR '>') AS parentcollections
FROM
    resources r
JOIN
    collectionelements ce ON r.resourceid = ce.resourceid
JOIN
    resources c ON ce.collectionid = c.resourceid
WHERE
    r.type = 'resource'
    AND r.resourceid = 11415;
 """
import mysql.connector
from dotenv import load_dotenv
import os
import pandas as pd

# load the environment variables
load_dotenv()

# connect to the database
cnx = mysql.connector.connect(user=os.getenv('MYSQL_USER'), password=os.getenv('MYSQL_PASSWORD'),
                              host=os.getenv('MYSQL_HOST'),
                              database=os.getenv('MYSQL_DATABASE'))
cursor = cnx.cursor()

# query to get the resources
resources_query = ("SELECT resourceid, title, pageurl, active, description, content, keywords FROM resources WHERE type = 'resource' AND active='T' limit 5")
resources_df = pd.read_sql(resources_query, cnx)

# create empty lists to store the education levels, subject areas and parent collections
edu_levels_list = []
subject_areas_list = []
parent_collections_list = []

# iterate over the resources dataframe to get the education levels, subject areas and parent collections
for index, row in resources_df.iterrows():
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

# merge the dataframes with the resources dataframe
resources_df = pd.merge(resources_df, edu_levels_df, on='resourceid', how='left')
resources_df = pd.merge(resources_df, subject_areas_df, on='resourceid', how='left')
resources_df = pd.merge(resources_df, parent_collections_df, on='resourceid', how='left')


print(resources_df.head())

cursor.close()
cnx.close()


