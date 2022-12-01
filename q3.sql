-- Rest Bylaw

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
drop table if exists q3 cascade;

create table q3(
    driver_id INTEGER,
    start DATE,
    driving INTERVAL,
    breaks INTERVAL
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS DriverWork CASCADE;
DROP VIEW IF EXISTS MinRest CASCADE;
DROP VIEW IF EXISTS Rested CASCADE;

-- Define views for your intermediate steps here:
CREATE VIEW DriverWork as
SELECT Dispatch.driver_id, 
       to_char(Pickup.datetime, 'YYYY-MM-DD') Pickup, 
       sum(Dropoff.datetime - Pickup.datetime) Duration
FROM Dispatch, Pickup, Dropoff
WHERE Dispatch.request_id = Pickup.request_id AND
      Pickup.request_id = Dropoff.request_id
GROUP BY Dispatch.driver_id, Pickup;

--SELECT * FROM DriverWork;
--------------------------------------------------------------------------------
CREATE VIEW MinRest as
SELECT min(Pickup.datetime - Dropoff.datetime) as RestSegment
FROM Dispatch, Pickup, Dropoff
WHERE Dispatch.request_id = Dropoff.request_id AND
      Pickup.request_id != Dropoff.request_id AND
      to_char(Pickup.datetime, 'YYYY-MM-DD') = 
      to_char(Dropoff.datetime, 'YYYY-MM-DD') AND 
      Pickup.datetime > Dropoff.datetime
GROUP BY Pickup;

--SELECT * FROM MinRest;
------------------------------------------------------------
CREATE VIEW Rested as
SELECT Dispatch.driver_id, 
       to_char(Pickup.datetime, 'YYYY-MM-DD') as Pickup, 
       (SELECT sum(RestSegment) FROM MinRest) as rest
FROM Dispatch, Pickup, Dropoff
WHERE Dispatch.request_id = Dropoff.request_id AND
      Pickup.request_id != Dropoff.request_id AND
      to_char(Pickup.datetime, 'YYYY-MM-DD') = 
      to_char(Dropoff.datetime, 'YYYY-MM-DD') AND 
      Pickup.datetime > Dropoff.datetime
GROUP BY Dispatch.driver_id, Pickup;

--SELECT * FROM Rested;
--------------------------------------------------------------------------------
CREATE VIEW OnlyOneRide as
SELECT Dispatch.driver_id, 
	to_char(Pickup.datetime, 'YYYY-MM-DD') as Pickup, 
	interval '00:00:00' as Rest 
FROM Dispatch, Pickup, Dropoff
WHERE Dispatch.request_id = Pickup.request_id AND

      Pickup.request_id = Dropoff.request_id AND

      to_char(Pickup.datetime, 'YYYY-MM-DD') = 
      to_char(Dropoff.datetime, 'YYYY-MM-DD') AND

      Pickup.datetime <= Dropoff.datetime AND

      to_char(Pickup.datetime, 'YYYY-MM-DD') != 
      ANY(SELECT Rested.pickup FROM Rested)
GROUP BY Dispatch.driver_id, Pickup;

--SELECT * FROM OnlyOneRide;
--------------------------------------------------------------------------------
CREATE VIEW DriverRest as
(SELECT * FROM Rested)
UNION
(SELECT * FROM OnlyOneRide);

--SELECT * FROM DriverRest;
--------------------------------------------------------------------------------
CREATE VIEW DriverSchedule as 
SELECT DW.driver_id, DW.pickup, DW.duration, DR.rest
FROM DriverWork DW, DriverRest DR
WHERE DW.driver_id = DR.driver_id AND
      DW.pickup = DR.pickup
ORDER BY DW.pickup;

--SELECT * FROM DriverSchedule;
--------------------------------------------------------------------------------
CREATE VIEW PotentialBreaker as
SELECT driver_id, pickup, duration, rest
FROM DriverSchedule
WHERE duration >= interval '12:00:00' AND rest <= interval '00:15:00';

--SELECT * FROM PotentialBreaker;
--------------------------------------------------------------------------------
CREATE VIEW ActualBreaker as
SELECT P1.driver_id as driver, 
       to_date(P1.pickup, 'YYYY-MM-DD') as start, 
       P1.duration + P2.duration + P3.duration as driving, 
       P1.rest + P2.rest + P3.rest as breaks
FROM PotentialBreaker P1, PotentialBreaker P2, PotentialBreaker P3
WHERE P1.driver_id = P2.driver_id AND 
      P2.driver_id = P3.driver_id AND
      P1.pickup < P2.pickup AND 
      P2.pickup < P3.pickup AND
      to_date(P1.pickup, 'YYYY-MM-DD') = 
                         to_date(P2.pickup, 'YYYY-MM-DD') - interval '1 day' AND
      to_date(P2.pickup, 'YYYY-MM-DD') = 
			 to_date(P3.pickup, 'YYYY-MM-DD') - interval '1 day';

--SELECT * FROM ActualBreaker;
--------------------------------------------------------------------------------

-- Your query that answers the question goes below the "insert into" line:
insert into q3
(SELECT * FROM ActualBreaker);

SELECT * FROM q3;
