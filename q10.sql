-- Rainmakers

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
drop table if exists q10 cascade;

create table q10(
	driver_id INTEGER,
	month CHAR(2),
	mileage_2014 FLOAT,
	billings_2014 FLOAT,
	mileage_2015 FLOAT,
	billings_2015 FLOAT,
	billings_increase FLOAT,
	mileage_increase FLOAT
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS Month CASCADE;
DROP VIEW IF EXISTS MilePre CASCADE;
DROP VIEW IF EXISTS DriverMonth CASCADE;
DROP VIEW IF EXISTS P1 CASCADE;
DROP VIEW IF EXISTS P2 CASCADE;
DROP VIEW IF EXISTS RequestLocation CASCADE;
DROP VIEW IF EXISTS MileAfter CASCADE;
DROP VIEW IF EXISTS Result CASCADE;

-- Define views for your intermediate steps here:
CREATE VIEW Month as
SELECT to_char(DATE '2014-01-01' + 
	(interval '1' month * generate_series(0,11)), 'MM') as mo;

CREATE VIEW DriverMonth as
SELECT Driver.driver_id, mo
FROM Driver,Month;

CREATE VIEW P1 as
SELECT *
FROM Place;

CREATE VIEW P2 as
SELECT *
FROM Place;

CREATE VIEW RequestLocation as
SELECT request_id, p1.location as source, p2.location as destination, datetime
FROM (Request JOIN P1 on Request.source = p1.name) JOIN 
	P2 on Request.destination = p2.name;

CREATE VIEW MilePre as
SELECT DriverMonth.driver_id, mo, 
	coalesce(sum(source <@> destination), 0) as miles, 
	coalesce(sum(amount), 0) as profit
FROM DriverMonth LEFT JOIN 
     (((RequestLocation RIGHT JOIN Dispatch 
	on RequestLocation.request_id = Dispatch.request_id)
     RIGHT JOIN Dropoff on RequestLocation.request_id = Dropoff.request_id) 
	RIGHT JOIN Billed on RequestLocation.request_id = Billed.request_id) 
on DriverMonth.driver_id = Dispatch.driver_id 
	and to_char(RequestLocation.datetime, 'MM') = DriverMonth.mo
	and date_part('year', RequestLocation.datetime) = 2014
GROUP BY DriverMonth.driver_id, mo;

CREATE VIEW MileAfter as
SELECT DriverMonth.driver_id, mo, 
	coalesce(sum(source <@> destination), 0) as miles, 
	coalesce(sum(amount), 0) as profit
FROM DriverMonth LEFT JOIN 
     (((RequestLocation RIGHT JOIN Dispatch 
	on RequestLocation.request_id = Dispatch.request_id)
     RIGHT JOIN Dropoff on RequestLocation.request_id = Dropoff.request_id) 
	RIGHT JOIN Billed on RequestLocation.request_id = Billed.request_id) 
on DriverMonth.driver_id = Dispatch.driver_id 
	and to_char(RequestLocation.datetime, 'MM') = DriverMonth.mo
	and date_part('year', RequestLocation.datetime) = 2015
GROUP BY DriverMonth.driver_id, mo;

CREATE VIEW Result as
SELECT MilePre.driver_id, MilePre.mo, MilePre.miles as pmiles, 
	MilePre.profit as pprofit, MileAfter.miles as amiles, 
	MileAfter.profit as aprofit, MileAfter.profit - MilePre.profit as pdif,
	MileAfter.miles - MilePre.miles as mdif
FROM MilePre JOIN MileAfter on (MilePre.driver_id = MileAfter.driver_id and 
	MilePre.mo = MileAfter.mo);


insert into q10
(SELECT *
FROM Result);

SELECT * FROM q10;
