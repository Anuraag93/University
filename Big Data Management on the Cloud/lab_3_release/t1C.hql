set hivevar:studentId=20642433; --Please replace it with your student id 

DROP TABLE ${studentId}_facebooktraffic;

CREATE TABLE ${studentId}_facebooktraffic AS
SELECT * FROM (
    SELECT * FROM ${studentId}_mytraffic 
    WHERE url LIKE "www.Facebook.com"
) ${studentId}_ftraffic
WHERE ${studentId}_ftraffic.time BETWEEN unix_timestamp("2014-02-14 00:00:00") AND unix_timestamp("2014-02-15 00:00:00");

-- Dump the output to file
INSERT OVERWRITE LOCAL DIRECTORY './task1c-out/'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
  SELECT * FROM ${studentId}_facebooktraffic;