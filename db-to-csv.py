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

load_dotenv()

cnx = mysql.connector.connect(user=os.getenv('MYSQL_USER'), password=os.getenv('MYSQL_PASSWORD'),
                              host=os.getenv('MYSQL_HOST'),
                              database=os.getenv('MYSQL_DATABASE'))
cursor = cnx.cursor()


query = ("SELECT resourceid, title, pageurl, active, description, content, keywords FROM resources WHERE type = 'resource' AND active='T' limit 5")
cursor.execute(query)
result = cursor.fetchall()

for row in result:
    resourceid = row[0]
    edu_level_q = ("SELECT e.`levelid`, e.`displayname` FROM `resource_educationlevels` AS el LEFT JOIN `educationlevels` AS e ON (el.`educationlevelid` = e.`levelid`) WHERE el.`resourceid` = " + str(resourceid) + ";")
    cursor.execute(edu_level_q)
    edu_level = cursor.fetchall()
    print(edu_level)
    print('---------------')

    sub_query = ("SELECT CONCAT(s.displayname, \" > \" ,sa.displayname) AS displayname, sa.subjectareaid FROM `resource_subjectareas` AS rs LEFT JOIN `subjectareas` AS sa ON (rs.`subjectareaid` = sa.`subjectareaid`) inner join subjects s on sa.subjectid = s.subjectid WHERE rs.`resourceid` = " + str(resourceid) + ";")
    cursor.execute(sub_query)
    sub_result = cursor.fetchall()
    print(sub_result)
    print('========================')

cursor.close()
cnx.close()


