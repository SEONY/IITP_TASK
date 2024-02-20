--####################################################################
--# Order Priority Checking Query (Q4)
--####################################################################

--# This query determines how well the order priority system 
--# is working and gives an assessment of customer satisfac-tion.

--###############################
--# Business Question
--###############################

--# The Order Priority Checking Query counts the number of orders 
--# ordered in a given quarter of a given year in which 
--# at least one lineitem was received by the customer 
--# later than its committed date. 
--# The query lists the count of such orders for each order priority 
--# sorted in ascending priority order.

\explain plan
select
    o_orderpriority,
    count(*) as order_count
from
    orders
where
      o_orderdate >= date '1993-07-01'
  and o_orderdate < date '1993-07-01' + interval '3' month
  and exists (
               select
                      *
                 from
                      lineitem
                where
                      l_orderkey = o_orderkey
                  and l_commitdate < l_receiptdate
             )
group by
    o_orderpriority
order by
    o_orderpriority;

--###############################
--# Functional Query Definition
--###############################

\set linesize 400
\set timing on;

--# result: 5 rows
--#  o_orderpriority | order_count 
--# -----------------+-------------
--#  1-URGENT        |       10594
--#  2-HIGH          |       10476
--#  3-MEDIUM        |       10410
--#  4-NOT SPECIFIED |       10556
--#  5-LOW           |       10487
\explain plan
select
    o_orderpriority,
    count(*) as order_count
from
    orders
where
      o_orderdate >= date '1993-07-01'
  and o_orderdate < date '1993-07-01' + interval '3' month
  and exists (
               select
                      *
                 from
                      lineitem
                where
                      l_orderkey = o_orderkey
                  and l_commitdate < l_receiptdate
             )
group by
    o_orderpriority
order by
    o_orderpriority;

--#################################
--# Report
--#################################

\set timing off;

INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q04', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;
