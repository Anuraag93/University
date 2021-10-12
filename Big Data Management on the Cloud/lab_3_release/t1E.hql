set hivevar:studentId=20642433; --Please replace it with your student id 

-- Don't forget to drop the tables you create here
DROP TABLE ${studentId}_ipcountries;
DROP TABLE ${studentId}_facebooktrafficcountries;
DROP TABLE ${studentId}_facebookregioncounts;

-- Task 1E step 2
-- Create a table with two columns - a list of ip addresses with
-- their associated region names.
CREATE TABLE ${studentId}_ipcountries AS
SELECT ${studentId}_myips.ipAddress AS ipAddress, ${studentId}_myregions.regionName AS regionName
FROM ${studentId}_myips CROSS JOIN ${studentId}_myregions
WHERE ${studentId}_myips.intAddress BETWEEN ${studentId}_myregions.intMin AND ${studentId}_myregions.intMax;

-- Task 1E step 3
-- Create a table with two columns - the name of a region for each hit to Facebook
-- between the two given dates, and the time of that visit.
CREATE TABLE ${studentId}_facebooktrafficcountries AS
SELECT ${studentId}_ipcountries.regionName AS regionName, ${studentId}_facetraffic.time AS time
FROM (
    SELECT ${studentId}_ftraffic.ipAddress AS ipAddress, ${studentId}_ftraffic.time AS time 
    FROM (
        SELECT ${studentId}_mytraffic.ipAddress AS ipAddress, ${studentId}_mytraffic.time AS time 
        FROM ${studentId}_mytraffic 
        WHERE url LIKE "www.Facebook.com"
    ) ${studentId}_ftraffic
    WHERE ${studentId}_ftraffic.time BETWEEN unix_timestamp("2014-02-14 00:00:00") AND unix_timestamp("2014-02-15 00:00:00")
) ${studentId}_facetraffic INNER JOIN ${studentId}_ipcountries
ON ${studentId}_facetraffic.ipAddress = ${studentId}_ipcountries.ipAddress;



-- Task 1E step 4
-- Create a table which contains the number of visits to Facebook from 
-- each country between the given dates.
CREATE TABLE ${studentId}_facebookregioncounts AS
SELECT ${studentId}_facebooktrafficcountries.regionName AS regionName, COUNT(1) AS count
FROM ${studentId}_facebooktrafficcountries
GROUP BY regionName 
ORDER BY count DESC, regionName ASC;


-- Task 1E step 5
-- Write the contents of the table created in step 4 to the directory './task1e-out/'
-- Dump the output to file
INSERT OVERWRITE LOCAL DIRECTORY './task1e-out/'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
  SELECT * FROM ${studentId}_facebookregioncounts;
