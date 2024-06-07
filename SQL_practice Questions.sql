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

------------------------------------------------------------------
-- Ankit Bansal SQL Question from Accenture
CREATE TABLE employees  (employee_id int,employee_name varchar(15), email_id varchar(15) );
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
