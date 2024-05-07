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

