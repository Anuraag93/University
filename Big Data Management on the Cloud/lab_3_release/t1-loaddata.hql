set hivevar:studentId=20642433; --Please replace it with your student id 

DROP TABLE ${studentId}_mydomains;
DROP TABLE ${studentId}_myips;
DROP TABLE ${studentId}_myregions;
DROP TABLE ${studentId}_mytraffic;


CREATE TABLE ${studentId}_mydomains (url STRING, category STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH './Input_data/1/domains.csv'
INTO TABLE ${studentId}_mydomains;

CREATE TABLE ${studentId}_myips (ipAddress STRING, intAddress BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH './Input_data/1/ips.csv'
INTO TABLE ${studentId}_myips;

CREATE TABLE ${studentId}_myregions (ipMin STRING, ipMax STRING, intMin BIGINT, intMax BIGINT, regionCode STRING, regionName STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH './Input_data/1/regions.csv'
INTO TABLE ${studentId}_myregions;

CREATE TABLE ${studentId}_mytraffic (url STRING, ipAddress STRING, time TIMESTAMP)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH './Input_data/1/traffic.csv'
INTO TABLE ${studentId}_mytraffic;