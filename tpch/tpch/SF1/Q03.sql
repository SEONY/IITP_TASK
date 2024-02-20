--####################################################################
--# Shipping Priority Query (Q3)
--####################################################################

--# This query retrieves the 10 unshipped orders with the highest value.

--###############################
--# Business Question
--###############################

--# The Shipping Priority Query retrieves the shipping priority 
--# and potential revenue, defined as 
--# the sum of l_extendedprice * (1-l_discount), 
--# of the orders having the largest revenue among those 
--# that had not been shipped as of a given date. 
--# Orders are listed in decreasing order of revenue. 
--# If more than 10 unshipped orders exist, 
--# only the 10 orders with the largest revenue are listed.

\explain plan
  select
      l_orderkey,
	  ROUND( sum(l_extendedprice*(1-l_discount)), 2) as revenue,
	  o_orderdate,
	  o_shippriority
  from
      customer,
	  orders,
	  lineitem
  where
        c_mktsegment = 'BUILDING'
    and c_custkey = o_custkey
	and l_orderkey = o_orderkey
    and o_orderdate < date '1995-03-15'
	and l_shipdate > date '1995-03-15'
  group by
      l_orderkey,
	  o_orderdate,
	  o_shippriority
  order by
      revenue desc,
      o_orderdate
  fetch 10;

--###############################
--# Functional Query Definition
--###############################

--# Return the first 10 selected rows


\set linesize 400
\set timing on;

--# result: 10 rows
--#  l_orderkey |  revenue  | o_orderdate | o_shippriority 
--# ------------+-----------+-------------+----------------
--#     2456423 | 406181.01 | 1995-03-05  |              0
--#     3459808 | 405838.70 | 1995-03-04  |              0
--#      492164 | 390324.06 | 1995-02-19  |              0
--#     1188320 | 384537.94 | 1995-03-09  |              0
--#     2435712 | 378673.06 | 1995-02-26  |              0
--#     4878020 | 378376.80 | 1995-03-12  |              0
--#     5521732 | 375153.92 | 1995-03-13  |              0
--#     2628192 | 373133.31 | 1995-02-22  |              0
--#      993600 | 371407.46 | 1995-03-05  |              0
--#     2300070 | 367371.15 | 1995-03-13  |              0

\explain plan
  select
      l_orderkey,
	  ROUND( sum(l_extendedprice*(1-l_discount)), 2) as revenue,
	  o_orderdate,
	  o_shippriority
  from
      customer,
	  orders,
	  lineitem
  where
        c_mktsegment = 'BUILDING'
    and c_custkey = o_custkey
	and l_orderkey = o_orderkey
    and o_orderdate < date '1995-03-15'
	and l_shipdate > date '1995-03-15'
  group by
      l_orderkey,
	  o_orderdate,
	  o_shippriority
  order by
      revenue desc,
      o_orderdate
  fetch 10;

--#################################
--# Report
--#################################

\set timing off;
INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q03', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;
