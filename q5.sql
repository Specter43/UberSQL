-- Bigger and smaller spenders

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
drop table if exists q5 cascade;

create table q5(
	client_id INTEGER,
	months VARCHAR(7),     
	total FLOAT,
	comparison VARCHAR(30)
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS UberMonthAverage CASCADE;
DROP VIEW IF EXISTS BilledClientReport CASCADE;
DROP VIEW IF EXISTS Result CASCADE;


-- Define views for your intermediate steps here:
CREATE VIEW UberMonthAverage as
SELECT to_char(D.datetime, 'YYYY MM') as month, 
       avg(B.amount) as average
FROM Dropoff D, Billed B
WHERE D.request_id = B.request_id
GROUP BY month;

--SELECT * FROM UberMonthAverage;
--------------------------------------------------------------------------------
CREATE VIEW BilledClientReport as
SELECT R.client_id, 
       to_char(D.datetime, 'YYYY MM') as month, 
       sum(B.amount) as total, 
       avg(B.amount) as average
FROM Dropoff D, Billed B, Request R
WHERE D.request_id = B.request_id AND
      B.request_id = R.request_id
GROUP BY client_id, month;

--SELECT * FROM BilledClientReport;
--------------------------------------------------------------------------------
CREATE VIEW Result as
SELECT B.client_id, 
       B.month as months, 
       B.total, 
       CASE
           WHEN B.average >= U.average THEN 'at or above'
           ELSE 'below'
       END as comparison
FROM UberMonthAverage U, BilledClientReport B
WHERE U.month = B.month;

--SELECT * FROM Result;
--------------------------------------------------------------------------------

-- Your query that answers the question goes below the "insert into" line:
insert into q5
(SELECT * FROM Result);

SELECT * FROM q5;
