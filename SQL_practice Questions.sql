---------------Important ---------
Amazon Interview Question - https://www.youtube.com/watch?v=d7pZNZbpdo8&t=22s
	
----- Problem 1 ------------
create table emp_2020
(
emp_id int,
designation varchar(20)
);

create table emp_2021
(
emp_id int,
designation varchar(20)
);


insert into emp_2020 values (1,'Trainee');
insert into emp_2020 values (2,'Developer');
insert into emp_2020 values (3,'Senior Developer');
insert into emp_2020 values (4,'Manager');

insert into emp_2021 values (1,'Developer'); 
insert into emp_2021 values (2,'Developer');
insert into emp_2021 values (3,'Manager');
insert into emp_2021 values (5,'Trainee');

SELECT * FROM EMP_2020;
SELECT * FROM EMP_2021;


with cte as(
SELECT 
    COALESCE(e20.emp_id, e21.emp_id) AS emp_id,
    e20.designation AS designation_2020,
    e21.designation AS designation_2021
FROM 
    emp_2020 e20
FULL OUTER JOIN 
    emp_2021 e21 ON e20.emp_id = e21.emp_id)
select emp_id,
case when designation_2020=designation_2021 then 'NOT PROMOTED'
when designation_2020 is null then 'NEW EMPLOYEE'
when designation_2021 is null then 'RESIGNED'
else 'PROMOTED' 
END as status
from cte

------- Problem 2 ------------
create table table_a
(
 empid int, empname varchar(50), 
 salary int
);

create table table_b
(
 empid int, 
 empname varchar(50), 
 salary int
);

insert into table_a values(1,'AA',1000);
insert into table_a values(2,'BB',300);
insert into table_b values(2,'BB',400);
insert into table_b values(3,'CC',100);

SELECT * FROM TABLE_A;
SELECT * FROM TABLE_B;

with cte as (
select coalesce(a.empid,b.empid) as empid, a.empname as e1,a.salary as s1,b.empname as e2,b.salary as s2
from table_a a full outer join table_b b on a.empid=b.empid)

select empid,
case when e1 is null then e2
else e1
end as empname,
case when s1 is null then s2
else s1
end as salary
from cte
-----------------problem 3--------------------------
CREATE TABLE city_distance
(
distance INT,
source VARCHAR(512),
destination VARCHAR(512)
);

delete from city_distance;
INSERT INTO city_distance(distance, source, destination) VALUES ('100', 'New Delhi', 'Panipat');
INSERT INTO city_distance(distance, source, destination) VALUES ('200', 'Ambala', 'New Delhi');
INSERT INTO city_distance(distance, source, destination) VALUES ('150', 'Bangalore', 'Mysore');
INSERT INTO city_distance(distance, source, destination) VALUES ('150', 'Mysore', 'Bangalore');
INSERT INTO city_distance(distance, source, destination) VALUES ('250', 'Mumbai', 'Pune');
INSERT INTO city_distance(distance, source, destination) VALUES ('250', 'Pune', 'Mumbai');
INSERT INTO city_distance(distance, source, destination) VALUES ('2500', 'Chennai', 'Bhopal');
INSERT INTO city_distance(distance, source, destination) VALUES ('2500', 'Bhopal', 'Chennai');
INSERT INTO city_distance(distance, source, destination) VALUES ('60', 'Tirupati', 'Tirumala');
INSERT INTO city_distance(distance, source, destination) VALUES ('80', 'Tirumala', 'Tirupati');
INSERT INTO city_distance(distance, source, destination) VALUES ('2500', 'x', 'y');

with cte as (select distance,lead(distance) over(partition by distance) as d,
			 source,lead(source) over(partition by distance) as s1,
			 destination,lead(destination) over(partition by distance) as d1
from city_distance)

select * from city_distance
except
select distance,source,destination
from cte where s1 = destination and d1 = source and d = distance

	-----------------problem 4--------------------------
CREATE TABLE service_status (
    service_name VARCHAR(50),
    updated_time TIMESTAMP,
    status VARCHAR(10)
);
INSERT INTO service_status (service_name, updated_time, status) VALUES 
('hdfs', '2024-03-06 10:00:00', 'up'),
('hdfs', '2024-03-06 10:01:00', 'up'),
('hdfs', '2024-03-06 10:02:00', 'down'),
('hdfs', '2024-03-06 10:03:00', 'down'),
('hdfs', '2024-03-06 10:04:00', 'down'),
('hdfs', '2024-03-06 10:05:00', 'down'),
('hdfs', '2024-03-06 10:06:00', 'down'),
('hdfs', '2024-03-06 10:07:00', 'up'),
('hdfs', '2024-03-06 10:08:00', 'up'),
('hdfs', '2024-03-06 10:09:00', 'down'),
('hdfs', '2024-03-06 10:10:00', 'down');

with cte as
(select service_name,updated_time as t1, 
lead(updated_time) over(partition by status order by updated_time) as t2,status
-- EXTRACT(MINUTE FROM (lead(updated_time) over(partition by status order by updated_time)- updated_time)) as dif,
-- row_number() over(partition by updated_time order by updated_time) as rn
from service_status where status='down'),

cte2 as(select cte.*,
EXTRACT(MINUTE FROM (t2-(select min(t1) as m from cte))) as di,
row_number() over(partition by cte.service_name) as r
from cte),

cte3 as (select service_name,t1,t2,status,(di-r) as x from cte2),

cte4 as (select x,count(*) as c from cte3 group by x),

cte5 as (select cte3.*, case when cte3.x= cte4.x then cte4.c end as t 
		 from cte3,cte4 where cte4.c>=3)
		 
select service_name,min(t1),max(t2),status
from cte5 where t is not null group by service_name,status
	
method-2:
with cte as (
select *, lag(status) over(order by updated_time) as prev_status from service_status
 ),
cte2 as (select *, 
sum
 (
 case when status='down' and prev_status='up' then 1 
 when status='up' and prev_status='down' then 1 else 0 end
 ) over(order by updated_time) 
 as group_key from cte)
,
cte3 as 
(
select service_name,min(status) as service_status ,min(updated_time) as start_updated_time,max(updated_time) as end_updated_time 
from cte2 group by group_key,service_name )
-- select * from cte3
select service_name,service_status,start_updated_time,end_updated_time from cte3 
where extract(minute from (end_updated_time - start_updated_time))>=3;

----------------Ankit Bansal SQL Question from Accenture-----------------------------------
CREATE TABLE employees  (
	employee_id int,employee_name varchar(15), email_id varchar(15) );
delete from employees;
INSERT INTO employees (employee_id,employee_name, email_id) VALUES ('101','Liam Alton', 'li.al@abc.com');
INSERT INTO employees (employee_id,employee_name, email_id) VALUES ('102','Josh Day', 'jo.da@abc.com');
INSERT INTO employees (employee_id,employee_name, email_id) VALUES ('103','Sean Mann', 'se.ma@abc.com'); 
INSERT INTO employees (employee_id,employee_name, email_id) VALUES ('104','Evan Blake', 'ev.bl@abc.com');
INSERT INTO employees (employee_id,employee_name, email_id) VALUES ('105','Toby Scott', 'jo.da@abc.com');
INSERT INTO employees (employee_id,employee_name, email_id) VALUES ('106','Anjali Chouhan', 'JO.DA@ABC.COM');
INSERT INTO employees (employee_id,employee_name, email_id) VALUES ('107','Ankit Bansal', 'AN.BA@ABC.COM');

with cte as(select *,row_number() over(partition by lower(email_id)) as ref from employees),
cte1 as (select *,case when ref>1 and email_id<>lower(email_id) then 0 else 1 end as x
from cte) 
select employee_id,employee_name, email_id from cte1 where x=1
	
----------------Ankit Bansal Fractal Analytics-----------------------------------------------------
 Create the 'king' table
CREATE TABLE king (
    k_no INT PRIMARY KEY,
    king VARCHAR(50),
    house VARCHAR(50)
);

-- Create the 'battle' table
CREATE TABLE battle (
    battle_number INT PRIMARY KEY,
    name VARCHAR(100),
    attacker_king INT,
    defender_king INT,
    attacker_outcome INT,
    region VARCHAR(50),
    FOREIGN KEY (attacker_king) REFERENCES king(k_no),
    FOREIGN KEY (defender_king) REFERENCES king(k_no)
);

delete from king;
INSERT INTO king (k_no, king, house) VALUES
(1, 'Robb Stark', 'House Stark'),
(2, 'Joffrey Baratheon', 'House Lannister'),
(3, 'Stannis Baratheon', 'House Baratheon'),
(4, 'Balon Greyjoy', 'House Greyjoy'),
(5, 'Mace Tyrell', 'House Tyrell'),
(6, 'Doran Martell', 'House Martell');

delete from battle;
-- Insert data into the 'battle' table
INSERT INTO battle (battle_number, name, attacker_king, defender_king, attacker_outcome, region) VALUES
(1, 'Battle of Oxcross', 1, 2, 1, 'The North'),
(2, 'Battle of Blackwater', 3, 4, 0, 'The North'),
(3, 'Battle of the Fords', 1, 5, 1, 'The Reach'),
(4, 'Battle of the Green Fork', 2, 6, 0, 'The Reach'),
(5, 'Battle of the Ruby Ford', 1, 3, 1, 'The Riverlands'),
(6, 'Battle of the Golden Tooth', 2, 1, 0, 'The North'),
(7, 'Battle of Riverrun', 3, 4, 1, 'The Riverlands'),
(8, 'Battle of Riverrun', 1, 3, 0, 'The Riverlands');
--for each region find house which has won maximum no of battles. display region, house and no of wins

select * from battle;
select * from king;

with cte as(
select *,case when attacker_outcome =0 then defender_king 
	else attacker_king end as win from battle),
cte1 as(select region,house,count(*) as cnt
from cte c join king k
on c.win = k.k_no group by 1,2),
cte2 as (select *,dense_rank() over(partition by region order by cnt desc) as d
from cte1)
select region,house,cnt as wins from cte2 where d=1
--------------------- sql 8 by TechTFQ ----------------------
drop table if exists job_skills;
create table job_skills
(
	row_id		int,
	job_role	varchar(20),
	skills		varchar(20)
);
insert into job_skills values (1, 'Data Engineer', 'SQL');
insert into job_skills values (2, null, 'Python');
insert into job_skills values (3, null, 'AWS');
insert into job_skills values (4, null, 'Snowflake');
insert into job_skills values (5, null, 'Apache Spark');
insert into job_skills values (6, 'Web Developer', 'Java');
insert into job_skills values (7, null, 'HTML');
insert into job_skills values (8, null, 'CSS');
insert into job_skills values (9, 'Data Scientist', 'Python');
insert into job_skills values (10, null, 'Machine Learning');
insert into job_skills values (11, null, 'Deep Learning');
insert into job_skills values (12, null, 'Tableau');

select * from job_skills;

with cte as (select *,
sum(case when job_role is not null then 1 else 0 end) over(order by row_id) as new
from job_skills)
select row_id,first_value(job_role) over(partition by new order by row_id) as job_role,skills
from cte

-- method 2 using recursive cte
with recursive cte as (
select * from job_skills where row_id=1
union
select js.row_id, coalesce(js.job_role,cte.job_role),js.skills from cte join job_skills js on js.row_id=cte.row_id+1)
select * from cte
--------------------SQL 9 TechTFQ------------------------------------------
drop TABLE if exists orders;
CREATE TABLE orders 
(
	customer_id 	INT,
	dates 			DATE,
	product_id 		INT
);
INSERT INTO orders VALUES
(1, '2024-02-18', 101),
(1, '2024-02-18', 102),
(1, '2024-02-19', 101),
(1, '2024-02-19', 103),
(2, '2024-02-18', 104),
(2, '2024-02-18', 105),
(2, '2024-02-19', 101),
(2, '2024-02-19', 106); 


with cte as(select *,lead(product_id) over(partition by dates,customer_id) as ld from orders)
(select dates,cast(product_id as text) from orders
union
select dates,case 
 when product_id<=ld then concat(product_id,',',ld)
 else concat(ld,',',product_id) end
 as projects_id from cte where ld is not null)
order by 1,2

-- Solution:
select dates, cast(product_id as varchar) as products 
from orders
union
select dates, string_agg(cast(product_id as varchar),',') as products
from orders
group by customer_id, dates
order by dates, products;

--------------------SQL 11 TechTFQ-----------------------------------------
drop table if exists hotel_ratings;
create table hotel_ratings
(
	hotel 		varchar(30),
	year		int,
	rating 		decimal
);
insert into hotel_ratings values('Radisson Blu', 2020, 4.8);
insert into hotel_ratings values('Radisson Blu', 2021, 3.5);
insert into hotel_ratings values('Radisson Blu', 2022, 3.2);
insert into hotel_ratings values('Radisson Blu', 2023, 3.8);
insert into hotel_ratings values('InterContinental', 2020, 4.2);
insert into hotel_ratings values('InterContinental', 2021, 4.5);
insert into hotel_ratings values('InterContinental', 2022, 1.5);
insert into hotel_ratings values('InterContinental', 2023, 3.8);

select * from hotel_ratings;

with cte as (select *,
round(avg(rating) over(partition by hotel),2) as avg_rt,
round(stddev(rating) over(partition by hotel),2) as std
-- if we place order by year the results will vary
from hotel_ratings),
cte1 as(select *,round(abs(rating-avg_rt)/std,2) as final from cte)
select hotel,year,rating from cte1 where final<1 

------------------ Amazon Interview Question from TechTFQ ------------------------------------
https://www.youtube.com/watch?v=d7pZNZbpdo8&t=22s
drop table if exists emp_attendance;
create table emp_attendance
(
	employee 	varchar(10),
	dates 		date,
	status 		varchar(20)
);
insert into emp_attendance values('A1', '2024-01-01', 'PRESENT');
insert into emp_attendance values('A1', '2024-01-02', 'PRESENT');
insert into emp_attendance values('A1', '2024-01-03', 'PRESENT');
insert into emp_attendance values('A1', '2024-01-04', 'ABSENT');
insert into emp_attendance values('A1', '2024-01-05', 'PRESENT');
insert into emp_attendance values('A1', '2024-01-06', 'PRESENT');
insert into emp_attendance values('A1', '2024-01-07', 'ABSENT');
insert into emp_attendance values('A1', '2024-01-08', 'ABSENT');
insert into emp_attendance values('A1', '2024-01-09', 'ABSENT');
insert into emp_attendance values('A1', '2024-01-010', 'PRESENT');
insert into emp_attendance values('A2', '2024-01-06', 'PRESENT');
insert into emp_attendance values('A2', '2024-01-07', 'PRESENT');
insert into emp_attendance values('A2', '2024-01-08', 'ABSENT');
insert into emp_attendance values('A2', '2024-01-09', 'PRESENT');
insert into emp_attendance values('A2', '2024-01-010', 'ABSENT');

select * from emp_attendance;

with cte as (select *,
lag(status) over(partition by employee order by dates) as lg
from emp_attendance),
cte1 as(select *,sum(case when (status='ABSENT' and lg='PRESENT') then 1 
when (status='PRESENT' and lg='ABSENT') then 1 else 0 end) 
over(partition by employee order by dates) as flag from cte)

select employee,min(dates) as from_date,max(dates) as to_date,status 
from cte1 group by employee,flag,status order by 1,2,3
	
-- Google SQL 𝐏𝐫𝐨𝐛𝐥𝐞𝐦 𝐒𝐭𝐚𝐭𝐞𝐦𝐞𝐧𝐭 --------------------
-- Count the occurances of words "sql" and "pyspark" in all of the text files.

create table google_files(
file_name varchar,
content varchar
);

insert into google_files(file_name , content) values('file1.txt', 'Google Uses SQL.')
, ('file2.txt','Google Uses SQL and PySpark to fetch the Data.'),
('file3.txt','Google Uses NoSQL DataBase and PySpark for processing of Data.')
,('file4.txt','Writing code in PySpark is very easy.');

select * from google_files;

with cte as(select sum(case when content like '% SQL%' then 1 else 0 end) as sql,
sum(case when content like '%PySpark%' then 1 else 0 end) as PySpark
from google_files group by file_name)
select 'SQL' as word ,sum(sql) as wordcount from cte
union
select 'PySpark' as word,sum(pyspark) as wordcount from cte

---------------------Leetcode 3140--------------------------------------------------------
use world;
CREATE TABLE if Not exists Cinema (
    seat_id INT PRIMARY KEY,
    free BOOLEAN
);

Truncate table Cinema;
insert into Cinema (seat_id, free) values ('1', '1');
insert into Cinema (seat_id, free) values ('2', '0');
insert into Cinema (seat_id, free) values ('3', '1');
insert into Cinema (seat_id, free) values ('4', '1');
insert into Cinema (seat_id, free) values ('5', '1');
insert into Cinema (seat_id, free) values ('6', '0');
insert into Cinema (seat_id, free) values ('7', '1');
insert into Cinema (seat_id, free) values ('8', '1');
insert into Cinema (seat_id, free) values ('9', '1');

select * from cinema;

with cte as (select *, lag(free) over(order by seat_id) as prev from cinema),
cte1 as (select *,sum(case when free=0 and prev=1 then 1
when free=1 and prev=0 then 1
else 0 end) over(order by seat_id) as cnt from cte),
cte2 as(select distinct min(seat_id) over(partition by cnt) as start, 
max(seat_id) over(partition by cnt)as end,
count(cnt) over(partition by cnt) as seats_count from cte1),
cte3 as(select *,dense_rank() over(order by seats_count desc) as rnk from cte2)
select * from cte3 where rnk=1
