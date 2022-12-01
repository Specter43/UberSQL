-- Months

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
drop table if exists q1 cascade;

create table q1(
    client_id INTEGER,
    email VARCHAR(30),
    months INTEGER
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
-- DROP VIEW IF EXISTS intermediate_step CASCADE;
DROP VIEW IF EXISTS RideMonth CASCADE;

-- Define views for your intermediate steps here:
CREATE VIEW RideMonth as
SELECT Client.client_id, Client.email, 
count(Distinct to_char(Request.datetime, 'YYYY-MM'))
FROM Client LEFT JOIN (Request RIGHT JOIN Dropoff on 
Request.request_id = Dropoff.request_id) on Client.client_id = Request.client_id
GROUP BY Client.client_id;

-- Your query that answers the question goes below the "insert into" line:
insert into q1
(SELECT *
FROM RideMonth);

SELECT * FROM q1;
