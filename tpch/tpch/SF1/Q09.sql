--####################################################################
--# Product Type Profit Measure Query (Q9)
--####################################################################

--# This query determines how much profit is made on a given line 
--# of parts, broken out by supplier nation and year.

--###############################
--# Business Question
--###############################

--# The Product Type Profit Measure Query finds, for each nation 
--# and each year, the profit for all parts ordered in that year 
--# that contain a specified substring in their names and 
--# that were filled by a supplier in that nation. 
--# The profit is defined as the sum of 
--# [(l_extendedprice*(1-l_discount)) - (ps_supplycost * l_quantity)] 
--# for all lineitems describing parts in the specified line. 
--# The query lists the nations in ascending alphabetical order and, 
--# for each nation, the year and profit in descending order by year 
--# (most recent first).

\explain plan
select
    nation,
    o_year,
    ROUND( sum(amount), 2 ) as sum_profit
from (
       select
           n_name as nation,
           extract(year from o_orderdate) as o_year,
           l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
       from
           part,
           supplier,
           lineitem,
           partsupp,
           orders,
           nation
       where
             s_suppkey = l_suppkey
         and ps_suppkey = l_suppkey
         and ps_partkey = l_partkey
         and p_partkey = l_partkey
         and o_orderkey = l_orderkey
         and s_nationkey = n_nationkey
         and p_name like '%green%'
     ) profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc;

--###############################
--# Functional Query Definition
--###############################

\set linesize 400
\set timing on;

--# result: 175 rows
--#           nation           | o_year | sum_profit  
--# ---------------------------+--------+-------------
--#  ALGERIA                   |   1998 | 27136900.18
--#  ALGERIA                   |   1997 |  48611833.5
--#  ......
--#  ......
--#  ......
--#  VIETNAM                   |   1993 | 45352676.87
--#  VIETNAM                   |   1992 | 47846355.65
\explain plan
select
    nation,
    o_year,
    ROUND( sum(amount), 2 ) as sum_profit
from (
       select
           n_name as nation,
           extract(year from o_orderdate) as o_year,
           l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
       from
           part,
           supplier,
           lineitem,
           partsupp,
           orders,
           nation
       where
             s_suppkey = l_suppkey
         and ps_suppkey = l_suppkey
         and ps_partkey = l_partkey
         and p_partkey = l_partkey
         and o_orderkey = l_orderkey
         and s_nationkey = n_nationkey
         and p_name like '%green%'
     ) profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc;

--#################################
--# Report
--#################################

\set timing off;

INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q09', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;
