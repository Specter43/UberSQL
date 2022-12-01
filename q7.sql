-- Rating histogram

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
drop table if exists q7 cascade;

create table q7(
    driver_id INTEGER,
    r5 INTEGER,
    r4 INTEGER,
    r3 INTEGER,
    r2 INTEGER,
    r1 INTEGER
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS Driver_Rating CASCADE;


-- Define views for your intermediate steps here:
CREATE VIEW Driver_Rating as
SELECT DP.driver_id, DR.rating
FROM Dispatch DP, DriverRating DR
WHERE DP.request_id = DR.request_id;

--SELECT * FROM Driver_Rating;
--------------------------------------------------------------------------------
CREATE VIEW RatingHistogram as
SELECT driver_id,
       count(case when rating = 5 then rating end) as r5, 
       count(case when rating = 4 then rating end) as r4, 
       count(case when rating = 3 then rating end) as r3, 
       count(case when rating = 2 then rating end) as r2, 
       count(case when rating = 1 then rating end) as r1
FROM Driver_Rating
GROUP BY driver_id;

--SELECT * FROM RatingHistogram;
--------------------------------------------------------------------------------
CREATE VIEW RatingHistogramWithNull as
SELECT driver_id,
       case when r5 != 0 then r5 else null end as r5,
       case when r4 != 0 then r4 else null end as r4,
       case when r3 != 0 then r3 else null end as r3,
       case when r2 != 0 then r2 else null end as r2,
       case when r1 != 0 then r1 else null end as r1
FROM RatingHistogram;

--SELECT * FROM RatingHistogramWithNull;
--------------------------------------------------------------------------------

-- Your query that answers the question goes below the "insert into" line:
insert into q7
(SELECT * FROM RatingHistogramWithNull);

SELECT * FROM q7;
