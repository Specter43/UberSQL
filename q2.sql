-- Lure them back

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
drop table if exists q2 cascade;

create table q2(
    client_id INTEGER,
    name VARCHAR(41),
    email VARCHAR(30),
    billed FLOAT,
    decline INTEGER
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS LargeAmountBefore CASCADE;
DROP VIEW IF EXISTS InCriteria CASCADE;
DROP VIEW IF EXISTS LateYear CASCADE;
DROP VIEW IF EXISTS Result CASCADE;
DROP VIEW IF EXISTS RequestCur CASCADE;
DROP VIEW IF EXISTS RequestPre CASCADE;

-- Define views for your intermediate steps here:
CREATE VIEW LargeAmountBefore as
SELECT Client.client_id, email, concat(firstname,' ',surname) as name, 
sum(amount) as billed
FROM Client LEFT JOIN 
	(Request RIGHT JOIN Billed on Request.request_id = Billed.request_id) 
on Client.client_id = Request.client_id
WHERE date_part('year', datetime) < 2014
GROUP BY Client.client_id
HAVING sum(amount) >= 500;

CREATE VIEW RequestPre as
SELECT *
FROM Request
WHERE date_part('year', datetime) = 2014;

CREATE VIEW RequestCur as
SELECT *
FROM Request
WHERE date_part('year', datetime) = 2015;

CREATE VIEW InCriteria as
SELECT Client.client_id, coalesce(count(amount), 0) as pre
FROM Client LEFT JOIN 
	(RequestPre RIGHT JOIN Billed 
		on RequestPre.request_id = Billed.request_id) 
on Client.client_id = RequestPre.client_id
GROUP BY Client.client_id
HAVING count(amount) <= 10 and count(amount) >= 1;

CREATE VIEW LateYear as
SELECT Client.client_id, coalesce(count(amount), 0) as cur
FROM Client LEFT JOIN 
	(RequestCur RIGHT JOIN Billed 
		on RequestCur.request_id = Billed.request_id) 
on Client.client_id = RequestCur.client_id
GROUP BY Client.client_id;

CREATE VIEW Result as
SELECT LargeAmountBefore.client_id, name, coalesce(email, 'unknown') as email, 
billed, (pre-cur) as decline
FROM (LargeAmountBefore JOIN InCriteria 
	on LargeAmountBefore.client_id = InCriteria.client_id) JOIN 
		LateYear on LargeAmountBefore.client_id = LateYear.client_id
WHERE pre-cur > 0;

-- Your query that answers the question goes below the "insert into" line:
insert into q2
(SELECT *
FROM Result);

SELECT * FROM q2;
