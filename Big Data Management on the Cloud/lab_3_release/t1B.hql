set hivevar:studentId=20642433; --Please replace it with your student id 

DROP TABLE ${studentId}_trafficcount;

CREATE TABLE ${studentId}_trafficcount AS
SELECT ${studentId}_mytraffic.url AS url, COUNT(1) AS count
FROM ${studentId}_mytraffic
GROUP BY url
ORDER BY count DESC;

-- Dump the output to file
INSERT OVERWRITE LOCAL DIRECTORY './task1b-out/'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
  SELECT * FROM ${studentId}_trafficcount;