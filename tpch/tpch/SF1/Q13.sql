--####################################################################
--# Customer Distribution Query (Q13)
--####################################################################

--# This query seeks relationships between customers and the size of 
--# their orders.

--###############################
--# Business Question
--###############################

--# This query determines the distribution of customers by the number 
--# of orders they have made, including customers who have no record 
--# of orders, past or present. 
--# It counts and reports how many customers have no orders, 
--# how many have 1, 2, 3, etc. 
--# A check is made to ensure that the orders counted do not fall 
--# into one of several special categories of orders. 
--# Special categories are identified in the order comment column 
--# by looking for a particular pattern.

\explain plan
select
    c_count, 
    count(*) as custdist
from (
       select
           c_custkey,
           count(o_orderkey) as c_count
       from
            customer left outer join orders 
            on c_custkey = o_custkey
            and o_comment not like '%special%requests%'
       group by
           c_custkey
     ) c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc;

--###############################
--# Functional Query Definition
--###############################

\set linesize 400
\set timing on;

--# result: 42 rows
--#  c_count | custdist 
--# ---------+----------
--#        0 |    50005
--#        9 |     6641
--#       10 |     6532
--# .....
--# .....
--# .....
--#       40 |        4
--#       41 |        2
--#       39 |        1
\explain plan
select
    c_count, 
    count(*) as custdist
from (
       select
           c_custkey,
           count(o_orderkey) as c_count
       from
            customer left outer join orders 
            on c_custkey = o_custkey
            and o_comment not like '%special%requests%'
       group by
           c_custkey
     ) c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc;

--#################################
--# Report
--#################################

\set timing off;

INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q13', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;
