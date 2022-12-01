-- Frequent riders

-- You must not change the next 2 lines or the table definition.
SET SEARCH_PATH TO uber, public;
drop table if exists q6 cascade;

create table q6(
	client_id INTEGER,
	year CHAR(4),
	rides INTEGER
);

-- Do this for each of the views that define your intermediate steps.  
-- (But give them better names!) The IF EXISTS avoids generating an error 
-- the first time this file is imported.
DROP VIEW IF EXISTS Combine CASCADE;
DROP VIEW IF EXISTS Year CASCADE;
DROP VIEW IF EXISTS ClientYear CASCADE;
DROP VIEW IF EXISTS CountAmount CASCADE;
DROP VIEW IF EXISTS Result CASCADE;


-- Define views for your intermediate steps here:
CREATE VIEW Year as
SELECT DISTINCT date_part('year', Request.datetime) as year
FROM Request;

CREATE VIEW ClientYear as 
SELECT client_id, year
FROM Client,Year;

CREATE VIEW CountAmount as
SELECT client_id, date_part('year', Request.datetime) as year, 
	count(amount) as amount
FROM Request RIGHT JOIN Billed on Request.request_id = Billed.request_id
GROUP BY client_id, date_part('year', Request.datetime)
ORDER BY client_id, date_part('year', Request.datetime);

CREATE VIEW Combine as
SELECT ClientYear.client_id, ClientYear.year, coalesce(amount, 0) as amount
FROM ClientYear LEFT JOIN CountAmount 
	on ClientYear.client_id = CountAmount.client_id 
		and CountAmount.year = ClientYear.year
ORDER BY ClientYear.client_id, CLientYear.year;

CREATE VIEW TopThree as
SELECT *
FROM Combine temp
WHERE (
	SELECT count(DISTINCT amount)
	FROM Combine
	WHERE year = temp.year and amount >= temp.amount) <=3;

CREATE VIEW BottomThree as
SELECT *
FROM Combine temp
WHERE (
	SELECT count(DISTINCT amount)
	FROM Combine
	WHERE year = temp.year and amount <= temp.amount) <=3;

CREATE VIEW Result as
(SELECT * FROM TopThree)
Union
(SELECT * FROM BottomThree);

-- Your query that answers the question goes below the "insert into" line:
insert into q6
(SELECT *
FROM Result);

SELECT * FROM q6;
