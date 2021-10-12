set hivevar:studentId=20642433; --Please replace it with your student id 
DROP TABLE ${studentId}_myinput;
DROP TABLE ${studentId}_mywords;
DROP TABLE ${studentId}_stopwords;
DROP TABLE ${studentId}_stopjoin;
DROP TABLE ${studentId}_stoplistout;

CREATE TABLE ${studentId}_myinput (line STRING);

-- Load the text from the local filesystem
LOAD DATA LOCAL INPATH './Input_data/2/'
  INTO TABLE ${studentId}_myinput;
  
CREATE TABLE ${studentId}_stopwords (word STRING);

-- Load the text from the local filesystem
LOAD DATA LOCAL INPATH './Input_data/4/'
  INTO TABLE ${studentId}_stopwords;

-- Table containing all the words in the myinput table
-- The difference between this table and myinput is that myinput stores each line as a separate row
-- whereas mywords stores each word as a separate row.
CREATE TABLE ${studentId}_mywords AS
SELECT EXPLODE(SPLIT(LCASE(REGEXP_REPLACE(line,'[\\p{Punct},\\p{Cntrl}]','')),' ')) AS word
FROM ${studentId}_myinput;
                                          
-- CREATE TABLE ${studentId}_stopjoin AS
-- SELECT ${studentId}_mywords.word AS mword, ${studentId}_stopwords.word AS sword
-- FROM ${studentId}_mywords LEFT OUTER JOIN ${studentId}_stopwords
-- ON (${studentId}_mywords.word = ${studentId}_stopwords.word)
-- WHERE ${studentId}_mywords.word NOT LIKE "";

-- Task 4 part 7 (1)
-- SELECT ${studentId}_mywords.word AS mword, ${studentId}_stopwords.word AS sword
-- FROM ${studentId}_mywords LEFT OUTER JOIN ${studentId}_stopwords
-- ON (${studentId}_mywords.word = ${studentId}_stopwords.word)
-- WHERE ${studentId}_mywords.word LIKE "the"
-- LIMIT 10;
                                          
-- -- Task 4 part 7 (2)
-- SELECT ${studentId}_mywords.word AS mword, ${studentId}_stopwords.word AS sword
-- FROM ${studentId}_mywords LEFT OUTER JOIN ${studentId}_stopwords
-- ON (${studentId}_mywords.word = ${studentId}_stopwords.word)
-- WHERE ${studentId}_mywords.word LIKE "help"
-- LIMIT 10;

-- Task 4 excercise 2
CREATE TABLE ${studentId}_stoplistout AS
SELECT ${studentId}_stopjoin.mword AS mword, COUNT(1) AS count 
FROM (
    SELECT ${studentId}_mywords.word AS mword, ${studentId}_stopwords.word AS sword
    FROM ${studentId}_mywords LEFT OUTER JOIN ${studentId}_stopwords
    ON (${studentId}_mywords.word = ${studentId}_stopwords.word)
    WHERE ${studentId}_mywords.word NOT LIKE ""
) ${studentId}_stopjoin
WHERE ${studentId}_stopjoin.sword IS NULL
GROUP BY mword
ORDER BY count DESC,mword ASC
LIMIT 10;

-- Dump the output to file
INSERT OVERWRITE LOCAL DIRECTORY './task4-out/'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
  SELECT * FROM ${studentId}_stoplistout;
