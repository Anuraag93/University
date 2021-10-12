set hivevar:studentId=20642433; --Please replace it with your student id 
DROP TABLE ${studentId}_myinput;
DROP TABLE ${studentId}_includewords;
DROP TABLE ${studentId}_includelistout;

CREATE TABLE ${studentId}_myinput (line STRING);

-- Load the text from the local filesystem
LOAD DATA LOCAL INPATH './Input_data/2/'
  INTO TABLE ${studentId}_myinput;
  
CREATE TABLE ${studentId}_includewords (word STRING);

-- Load the text from the local filesystem
LOAD DATA LOCAL INPATH './Input_data/6/'
  INTO TABLE ${studentId}_includewords;

-- Table containing all the words in the myinput table
-- The difference between this table and myinput is that myinput stores each line as a separate row
-- whereas mywords stores each word as a separate row.
                                      
-- Task 6
CREATE TABLE ${studentId}_includelistout AS
SELECT ${studentId}_includejoin.mword AS mword, COUNT(1) AS count 
FROM (
    SELECT ${studentId}_mywords.word AS mword, ${studentId}_includewords.word AS iword
    FROM (
        SELECT EXPLODE(SPLIT(LCASE(REGEXP_REPLACE(line,'[\\p{Punct},\\p{Cntrl}]','')),' ')) AS word
        FROM ${studentId}_myinput
    )${studentId}_mywords INNER JOIN ${studentId}_includewords
    ON (${studentId}_mywords.word = ${studentId}_includewords.word)
) ${studentId}_includejoin
GROUP BY mword
ORDER BY count DESC,mword ASC
LIMIT 10;

-- Dump the output to file
INSERT OVERWRITE LOCAL DIRECTORY './task6-out/'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
  SELECT * FROM ${studentId}_includelistout;
