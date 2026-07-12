create table customers (customer_id int, name char(50), email char(100));
create table orders (order_id int, customer_id int, total_amount float);
insert into customers values (1, 'Alice', 'alice@example.com');
insert into customers values (2, 'Bob', 'bob@example.com');
insert into customers values (3, 'Carol', 'carol@example.com');
insert into orders values (101, 1, 500.0);
insert into orders values (102, 1, 1200.0);
insert into orders values (103, 2, 900.0);
insert into orders values (104, 2, 1500.0);
insert into orders values (105, 3, 700.0);
select c.name, o.order_id from customers c join orders o on c.customer_id = o.customer_id where o.total_amount > 1000;
EXPLAIN ANALYZE select c.name, o.order_id from customers c join orders o on c.customer_id = o.customer_id where o.total_amount > 1000;
create index orders(order_id);
select c.name, o.order_id from customers c join orders o on c.customer_id = o.customer_id where o.total_amount > 1000;
EXPLAIN ANALYZE select c.name, o.order_id from customers c join orders o on c.customer_id = o.customer_id where o.total_amount > 1000;

create table a (x int);
create table b (x int);
insert into a values (1);
insert into a values (2);
insert into a values (3);
insert into b values (2);
insert into b values (3);
insert into b values (4);
select * from (select * from a union select * from b) as u order by x desc;

begin;
insert into a values (5);
select * from a where x = 5;
commit;
select * from a where x = 5;

begin;
insert into a values (6);
abort;
select * from a where x = 6;
