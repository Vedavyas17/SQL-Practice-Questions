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

----- Problem 2 ------------