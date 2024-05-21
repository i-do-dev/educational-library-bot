import mysql.connector
from dotenv import load_dotenv
import os
import sqlite3
import pandas as pd

load_dotenv()

# connect to the database
cnx = mysql.connector.connect(user=os.getenv('MYSQL_USER'), password=os.getenv('MYSQL_PASSWORD'),
                              host=os.getenv('MYSQL_HOST'),
                              database=os.getenv('MYSQL_DATABASE'))
cursor = cnx.cursor()

db_connection = sqlite3.connect('curriki.db')
cursor_lite = db_connection.cursor()

# delete all records from the table 'processed_resourcefile'
""" cursor_lite.execute('DELETE FROM processed_resourcefile;')
db_connection.commit()
exit() """

cursor_lite.execute('SELECT * FROM processed_resourcefile;')
processed_resourcefile_id = cursor_lite.fetchone()[0]

# query to get the resourcefiles
resourcefiles_query = (f"""
    SELECT 
        rf.fileid, rf.resourceid, rf.filename, rf.ext, rf.s3path,
        r.title, r.pageurl, r.active, r.keywords
    FROM 
        resourcefiles rf
    LEFT JOIN 
        resources r ON rf.resourceid = r.resourceid
    WHERE 
        r.type = 'resource' AND r.active = 'T' AND rf.ext = 'pdf' AND s3path IS NOT NULL AND s3path <> '' AND rf.fileid <= {processed_resourcefile_id} 
    ORDER BY rf.fileid
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

# close the database connection
cnx.close()
db_connection.close()

# resourcefiles_df to csv file
resourcefiles_df.to_csv('processed_resourcefiles.csv', index=False)
