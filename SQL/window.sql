show databases;

create database student;
use student;

CREATE TABLE marks (
    student_id INTEGER PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255),
    branch VARCHAR(255),
    marks INTEGER
);

INSERT INTO marks (name,branch,marks)VALUES 
('Nitish','EEE',82),
('Rishabh','EEE',91),
('Anukant','EEE',69),
('Rupesh','EEE',55),
('Shubham','CSE',78),
('Ved','CSE',43),
('Deepak','CSE',98),
('Arpan','CSE',95),
('Vinay','ECE',95),
('Ankit','ECE',88),
('Anand','ECE',81),
('Rohit','ECE',95),
('Prashant','MECH',75),
('Amit','MECH',69),
('Sunny','MECH',39),
('Gautam','MECH',51);

select * from marks;
select avg(marks) from marks; -- output : a single row 

select *,avg(marks) over() from marks; -- output : for every row. When u use aggragete function with over() clause it acts like window function.dual

select *,avg(marks) over(partition by branch) from marks; -- avg marks wr to branch

select *,min(marks) over(),max(marks) over() from marks;

-- find all the students who have marks higher then the average marks of their respective branch
select * from
(select *, avg(marks) over(partition by branch) as avg
from marks) t 
where t.marks>t.avg;

-- Order by marks using rand()
select *,
rank() over(order by marks desc)
from marks;

-- order by student marks in each branch 
select *,
	rank() over (partition by branch order by marks desc) as rank_branch,
	dense_rank() over (partition by branch order by marks desc) as denserank_branch
from marks;

-- Give number to every row 
select *,
	row_number() over (partition by branch)
from marks;

select *,
	concat(branch,'-',row_number() over(partition by branch))
from marks;

-- Find the branch toppers and last ones.
Select *,
first_value(name) over(partition by branch order by marks desc)
From marks;

select name, marks, branch from (Select *,
first_value(name) over(partition by branch order by marks desc) as topper_name,
first_value(marks) over(partition by branch order by marks desc) as topper_marks  
from marks) t
where t.name = t.topper_name AND t.marks = t.topper_marks;

select name, marks, branch from (Select *,
last_value(name) over(partition by branch order by marks desc
						ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as topper_name,
last_value(marks) over(partition by branch order by marks desc
						 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as topper_marks  
from marks) t
where t.name = t.topper_name AND t.marks = t.topper_marks;

select name,marks,branch from (Select *,
last_value(name) over W as topper_name,
last_value(marks) over W as topper_marks  
from marks
Window W as (partition by branch order by marks desc
						 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) t
where t.name = t.topper_name AND t.marks = t.topper_marks

-- Find the topper from last 

select *,
last_value(name) over(order by marks desc)
from marks; -- this query will give incorrect answer

use student;
Select *,
last_value(name) over(partition by branch order by marks desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
from marks; -- this is the correct way

-- Find the 2nd topper from the table 
Select *,
NTH_VALUE(name,2) over(partition by  branch order by marks desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
from marks;

-- using lead and lag 

select *,
lag(marks) over(partition by branch order by marks desc), -- students will get to know who is ahead of them 
lead(marks) over(partition by branch order by marks desc) -- students will get to know who is behind them 
from marks;  

-- USING ZOMATO DATA NOW 

use zomato;

-- every months top 2 customers

select * from orders;

select * from (select month(date) as month, user_id, sum(amount) as total,
				rank() over(partition by month(date) order by sum(amount) desc) as monthly_rank
				from orders 
				group by user_id, month(date)) as t
where t.monthly_rank<3;

-- find month on month revenue growth of zomato 

select * from orders;

select r_id, month(date), sum(amount) as amount,
lag(sum(amount)) over(partition by r_id order by month(date)) as lag_value,
((sum(amount) - lag(sum(amount)) over(partition by r_id order by month(date)))/lag(sum(amount)) over(partition by r_id order by month(date)))*100  as growth
from orders
group by r_id, month(date);
