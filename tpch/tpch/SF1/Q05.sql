--####################################################################
--# Local Supplier Volume Query (Q5)
--####################################################################

--# This query lists the revenue volume done through local suppliers.

--###############################
--# Business Question
--###############################

--# The Local Supplier Volume Query lists for each nation 
--# in a region the revenue volume that resulted 
--# from lineitem transactions in which the customer ordering parts 
--# and the supplier filling them were both within that nation. 
--# The query is run in order to determine whether to institute 
--# local distribution centers in a given region. 
--# The query consid-ers only parts ordered in a given year. 
--# The query displays the nations and revenue volume 
--# in descending order by revenue. 
--# Revenue volume for all qualifying lineitems in a particular nation 
--# is defined as sum(l_extendedprice * (1 - l_discount)).

\explain plan
select
    n_name,
    ROUND( sum(l_extendedprice * (1 - l_discount)), 2) as revenue
from
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
where
      c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and l_suppkey = s_suppkey
  and c_nationkey = s_nationkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'ASIA'
  and o_orderdate >= date '1994-01-01'
  and o_orderdate < date '1994-01-01' + interval '1' year
group by
    n_name
order by
    revenue desc;

--###############################
--# Functional Query Definition
--###############################

\set linesize 400
\set timing on;

--# result: 5 rows
--#          n_name           |   revenue   
--# ---------------------------+-------------
--#  INDONESIA                 | 55502041.17
--#  VIETNAM                   | 55295087.00
--#  CHINA                     | 53724494.26
--#  INDIA                     | 52035512.00
--#  JAPAN                     | 45410175.70

\explain plan
select
    n_name,
    ROUND( sum(l_extendedprice * (1 - l_discount)), 2) as revenue
from
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
where
      c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and l_suppkey = s_suppkey
  and c_nationkey = s_nationkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'ASIA'
  and o_orderdate >= date '1994-01-01'
  and o_orderdate < date '1994-01-01' + interval '1' year
group by
    n_name
order by
    revenue desc;

--#################################
--# Report
--#################################

\set timing off;
INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q05', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;
