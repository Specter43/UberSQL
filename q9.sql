-- Consistent raters

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
drop table if exists q9 cascade;

create table q9(
    client_id INTEGER,
    email VARCHAR(30)
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS RequestRated CASCADE;


-- Define views for your intermediate steps here:
CREATE VIEW RequestRated as
SELECT R.client_id, DR.request_id, DP.driver_id
FROM Request R, DriverRating DR, Dispatch DP
WHERE R.request_id = DR.request_id AND
DR.request_id = DP.request_id;

--SELECT * FROM RequestRated;
--------------------------------------------------------------------------------
CREATE VIEW ClientRequested as
SELECT Request.client_id, Request.request_id, RequestRated.driver_id
FROM Request, RequestRated
WHERE Request.client_id = RequestRated.client_id;

--SELECT * FROM ClientRequested;
--------------------------------------------------------------------------------
CREATE VIEW ConsistentRaters as
SELECT Client.client_id, Client.email
FROM Client, ClientRequested, RequestRated
WHERE Client.client_id = ClientRequested.client_id AND
      ClientRequested.client_id = RequestRated.client_id
GROUP BY Client.client_id
HAVING count(DISTINCT RequestRated.driver_id) =
       count(DISTINCT ClientRequested.driver_id);

--SELECT * FROM ConsistentRaters;
--------------------------------------------------------------------------------

-- Your query that answers the question goes below the "insert into" line:
insert into q9
(SELECT * FROM ConsistentRaters);

SELECT * FROM q9;
