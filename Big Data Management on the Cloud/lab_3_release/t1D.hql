set hivevar:studentId=20642433; --Please replace it with your student id 

DROP TABLE ${studentId}_trafficcatcount;

CREATE TABLE ${studentId}_trafficcatcount AS
SELECT ${studentId}_trafficcategory.category AS category, COUNT(1) AS count 
FROM (
    SELECT ${studentId}_mydomains.category AS category 
    FROM ${studentId}_mytraffic INNER JOIN ${studentId}_mydomains
    ON ${studentId}_mytraffic.url = ${studentId}_mydomains.url
) ${studentId}_trafficcategory
GROUP BY ${studentId}_trafficcategory.category
ORDER BY count DESC, category ASC
LIMIT 5;

-- Dump the output to file
INSERT OVERWRITE LOCAL DIRECTORY './task1d-out/'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
  SELECT * FROM ${studentId}_trafficcatcount;