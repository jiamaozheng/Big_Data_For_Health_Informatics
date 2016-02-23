-- ***************************************************************************
-- Loading Data:
-- create external table mapping for events.csv and mortality_events.csv
-- Note that events.csv is put under hdfs directory 'input/events/events.csv'
-- similar for mortality.csv
-- ***************************************************************************
-- create events table 
DROP TABLE IF EXISTS events;
CREATE EXTERNAL TABLE events (
  patient_id STRING,
  event_id STRING,
  event_description STRING,
  time DATE,
  value DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
--LOCATION 'input/events'
LOCATION '/user/vagrant/input/events'
TBLPROPERTIES ("skip.header.line.count"="1");

-- create mortality events table 
DROP TABLE IF EXISTS mortality;
CREATE EXTERNAL TABLE mortality (
  patient_id STRING,
  time DATE,
  label INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
--LOCATION 'input/mortality'
LOCATION '/user/vagrant/input/events'
TBLPROPERTIES ("skip.header.line.count"="1");


-- ******************************************************
-- Task 1:
-- By manipulating the above two tables, 
-- generate two views for alive and dead patients' events
-- ******************************************************
-- find events for alive patients
DROP VIEW IF EXISTS alive_events;
CREATE VIEW alive_events 
AS
SELECT events.patient_id, events.event_id, events.time 
-- ***** your code below *****

FROM events 
WHERE events.patient_id 
NOT IN ( SELECT mortality.patient_id FROM mortality );

-- find events for dead patients
DROP VIEW IF EXISTS dead_events;
CREATE VIEW dead_events 
AS
SELECT events.patient_id, events.event_id, events.time
-- ***** your code below *****

FROM events
INNER JOIN mortality
ON mortality.patient_id = events.patient_id;

-- ************************************************
-- Task 2: Event count metrics
-- Compute average, min and max of event counts 
-- for alive and dead patients respectively  
-- ************************************************
-- alive patients
SELECT avg(event_count), min(event_count), max(event_count)
-- ***** your code below *****

FROM (SELECT count(alive_events.event_id) AS event_count
	  FROM alive_events GROUP BY alive_events.patient_id) alive_event_count;

-- dead patients
SELECT avg(event_count), min(event_count), max(event_count)
-- ***** your code below *****

FROM (SELECT count(dead_events.event_id) AS event_count
	  FROM dead_events GROUP BY dead_events.patient_id) dead_event_count;

-- ************************************************
-- Task 3: Encounter count metrics 
-- Compute average, min and max of encounter counts 
-- for alive and dead patients respectively
-- ************************************************
-- alive
SELECT avg(encounter_count), min(encounter_count), max(encounter_count)
-- ***** your code below *****

FROM (SELECT count(DISTINCT alive_events.time) AS encounter_count 
	  FROM alive_events GROUP BY alive_events.patient_id) alive_encounter_count;	

-- dead
SELECT avg(encounter_count), min(encounter_count), max(encounter_count)
-- ***** your code below *****

FROM (SELECT count(DISTINCT dead_events.time) AS encounter_count 
	  FROM dead_events GROUP BY dead_events.patient_id) dead_encounter_count;

-- ************************************************
-- Task 4: Record length metrics
-- Compute average, min and max of record lengths
-- for alive and dead patients respectively
-- ************************************************
-- alive 
SELECT avg(record_length), min(record_length), max(record_length)
-- ***** your code below *****
FROM (SELECT datediff(max_time, min_time) AS record_length
	  FROM (SELECT max(alive_events.time) AS max_time, min(alive_events.time) AS min_time
	  FROM alive_events GROUP BY alive_events.patient_id) min_max alive_time) alive_record_length;

-- dead
SELECT avg(record_length), min(record_length), max(record_length)
-- ***** your code below *****
FROM (SELECT datediff(max_time, min_time) AS record_length
	  FROM (SELECT max(dead_events.time) AS max_time, min(dead_events.time) AS min_time
	  FROM dead_events GROUP BY dead_events.patient_id) min_max dead_time) dead_record_length;

-- ******************************************* 
-- Task 5: Common diag/lab/med
-- Compute the 5 most frequently occurring diag/lab/med
-- for alive and dead patients respectively
-- *******************************************
-- alive patients
---- diag
SELECT event_id, count(*) AS diag_count
FROM alive_events 
-- ***** your code below *****
WHERE event_id LIKE 'DIAG%'
GROUP BY event_id
ORDER BY diag_count
LIMIT 5;

---- lab
SELECT event_id, count(*) AS lab_count
FROM alive_events
-- ***** your code below *****
WHERE event_id LIKE 'LAB%'
GROUP BY event_id
ORDER BY diag_count
LIMIT 5;

---- med
SELECT event_id, count(*) AS med_count
FROM alive_events
-- ***** your code below *****
WHERE event_id LIKE 'DRUG%'
GROUP BY event_id
ORDER BY diag_count
LIMIT 5;



-- dead patients
---- diag
SELECT event_id, count(*) AS diag_count
FROM dead_events
-- ***** your code below *****
WHERE event_id LIKE 'DIAG%'
GROUP BY event_id
ORDER BY diag_count
LIMIT 5;

---- lab
SELECT event_id, count(*) AS lab_count
FROM dead_events
-- ***** your code below *****
WHERE event_id LIKE 'LAB%'
GROUP BY event_id
ORDER BY diag_count
LIMIT 5;


---- med
SELECT event_id, count(*) AS med_count
FROM dead_events
-- ***** your code below *****
WHERE event_id LIKE 'DRUG%'
GROUP BY event_id
ORDER BY diag_count
LIMIT 5;






