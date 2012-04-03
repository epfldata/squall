--
-- Lineitem table
--
select l_partkey, count(*) as cnt from lineitem group by l_partkey order by cnt desc
go
select l_orderkey, count(*) as cnt from lineitem group by l_orderkey order by cnt desc
go
select l_shipdate, count(*) as cnt from lineitem group by l_shipdate order by cnt desc
go
select l_commitdate, count(*) as cnt from lineitem group by l_commitdate order by cnt desc
go
select l_receiptdate, count(*) as cnt from lineitem group by l_receiptdate order by cnt desc
go
select l_returnflag, count(*) as cnt from lineitem group by l_returnflag order by cnt desc
go

--
-- Orders table
--
select o_custkey, count(*) as cnt from orders group by o_custkey order by cnt desc
go
select o_orderdate, count(*) as cnt from orders group by o_orderdate order by cnt desc
go
select o_shippriority, count(*) as cnt from orders group by o_shippriority order by cnt desc
go

-- 
-- Customer table
-- 
select c_nationkey, count(*) as cnt from customer group by c_nationkey order by cnt desc
go
select c_acctbal, count(*) as cnt from customer group by c_acctbal order by cnt desc
go
select c_mktsegment, count(*) as cnt from customer group by c_mktsegment order by cnt desc
go

-- 
-- Supplier table
--
select s_nationkey, count(*) as cnt from supplier group by s_nationkey order by cnt desc
go
select s_acctbal, count(*) as cnt from supplier group by s_acctbal order by cnt desc
go

--
-- Partsupp table
--
select ps_supplycost, count(*) as cnt from partsupp group by ps_supplycost order by cnt desc
go
select ps_availqty, count(*) as cnt from partsupp group by ps_availqty order by cnt desc
go

--
-- Part table
--
select p_size, count(*) as cnt from part group by p_size order by cnt desc
go
select p_brand, count(*) as cnt from part group by p_brand order by cnt desc
go
