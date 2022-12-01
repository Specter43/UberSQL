-- Do drivers improve?

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
drop table if exists q4 cascade;

create table q4(
	type VARCHAR(9),
	number INTEGER,
	early FLOAT,
	late FLOAT
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS Result CASCADE;
DROP VIEW IF EXISTS MoreThanTenDays CASCADE;
DROP VIEW IF EXISTS LastFive CASCADE;
DROP VIEW IF EXISTS FirstFive CASCADE;

-- Define views for your intermediate steps here:

CREATE VIEW MoreThanTenDays as
SELECT Driver.driver_id, trained, min(Request.datetime) as firstday
FROM Driver LEFT JOIN 
       (Request RIGHT JOIN Dispatch on Request.request_id = Dispatch.request_id) 
on Driver.driver_id = Dispatch.driver_id
GROUP BY Driver.driver_id
HAVING count(DISTINCT to_char(Request.datetime, 'YYYY-MM-DD')) >= 10;

CREATE VIEW FirstFive as
SELECT avg(rating) as firstrate, MoreThanTenDays.driver_id
FROM (MoreThanTenDays LEFT JOIN 
      ((Request RIGHT JOIN Dispatch on Request.request_id = Dispatch.request_id)
	RIGHT JOIN Dropoff on Request.request_id = Dropoff.request_id) 
on MoreThanTenDays.driver_id = Dispatch.driver_id) LEFT JOIN DriverRating
	on Request.request_id = DriverRating.request_id
WHERE Request.datetime - firstday < interval '5 day'
	and Request.datetime >= firstday
GROUP BY MoreThanTenDays.driver_id;

CREATE VIEW LateFive as
SELECT avg(rating) as laterate, MoreThanTenDays.driver_id
FROM (MoreThanTenDays LEFT JOIN 
      ((Request RIGHT JOIN Dispatch on Request.request_id = Dispatch.request_id)
	RIGHT JOIN Dropoff on Request.request_id = Dropoff.request_id)  
on MoreThanTenDays.driver_id = Dispatch.driver_id) LEFT JOIN DriverRating
	on Request.request_id = DriverRating.request_id
WHERE Request.datetime - firstday >= interval '5 day'
GROUP BY MoreThanTenDays.driver_id; 

CREATE VIEW Result as
SELECT Case
		WHEN trained Then 'trained'
		ELSE 'untrained'
        END as train, count(FirstFive.driver_id) as number, 
	avg(firstrate) as early, avg(laterate) as late	
FROM (FirstFive NATURAL JOIN LateFive) LEFT JOIN Driver 
	on FirstFive.driver_id = Driver.driver_id
GROUP BY trained;

-- Your query that answers the question goes below the "insert into" line:
insert into q4
(SELECT *
FROM Result);

SELECT * FROM q4;
