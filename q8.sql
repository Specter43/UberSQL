-- Scratching backs?

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
drop table if exists q8 cascade;

create table q8(
    client_id INTEGER,
    reciprocals INTEGER,
    difference FLOAT
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS IsReciprocal CASCADE;
DROP VIEW IF EXISTS Result CASCADE;

-- Define views for your intermediate steps here:
CREATE VIEW IsReciprocal as 
SELECT DriverRating.request_id, DriverRating.rating as driver, 
	ClientRating.rating as client
FROM DriverRating JOIN ClientRating 
	on DriverRating.request_id = ClientRating.request_id;

CREATE VIEW Result as
SELECT client_id, count(Request.request_id), avg(driver - client)
FROM IsReciprocal LEFT JOIN Request 
	on IsReciprocal.request_id = Request.request_id
GROUP BY client_id;

-- Your query that answers the question goes below the "insert into" line:
insert into q8
(SELECT *
FROM Result);

SELECT * FROM q8;
