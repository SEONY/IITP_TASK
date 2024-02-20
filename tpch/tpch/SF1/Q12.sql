--####################################################################
--# Shipping Modes and Order Priority Query (Q12)
--####################################################################

--# This query determines whether selecting less expensive modes of 
--# shipping is negatively affecting the critical-prior-ity 
--# orders by causing more parts to be received by customers 
--# after the committed date.

--###############################
--# Business Question
--###############################

--# The Shipping Modes and Order Priority Query counts, by ship mode, 
--# for lineitems actually received by customers in a given year, 
--# the number of lineitems belonging to orders for which the 
--# l_receiptdate exceeds the l_commitdate for two different specified 
--# ship modes. 
--# Only lineitems that were actually shipped before the l_commitdate 
--# are con-sidered. 
--# The late lineitems are partitioned into two groups, those with 
--# priority URGENT or HIGH, and those with a priority other than 
--# URGENT or HIGH.

\explain plan 
select
    l_shipmode,
    sum( case
             when o_orderpriority ='1-URGENT'
                  or o_orderpriority ='2-HIGH'
             then 1
             else 0
         end ) as high_line_count,
    sum( case
             when o_orderpriority <> '1-URGENT'
              and o_orderpriority <> '2-HIGH'
             then 1
             else 0
         end ) as low_line_count
from
    orders,
    lineitem
where
      o_orderkey = l_orderkey
  and l_shipmode in ('MAIL', 'SHIP')
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date '1994-01-01'
  and l_receiptdate < date '1994-01-01' + interval '1' year
group by
   l_shipmode
order by
   l_shipmode;

--###############################
--# Functional Query Definition
--###############################

\set linesize 400
\set timing on;

--# result: 2 rows
--#  l_shipmode | high_line_count | low_line_count 
--# ------------+-----------------+----------------
--#  MAIL       |            6202 |           9324
--#  SHIP       |            6200 |           9262

\explain plan 
select
    l_shipmode,
    sum( case
             when o_orderpriority ='1-URGENT'
                  or o_orderpriority ='2-HIGH'
             then 1
             else 0
         end ) as high_line_count,
    sum( case
             when o_orderpriority <> '1-URGENT'
              and o_orderpriority <> '2-HIGH'
             then 1
             else 0
         end ) as low_line_count
from
    orders,
    lineitem
where
      o_orderkey = l_orderkey
  and l_shipmode in ('MAIL', 'SHIP')
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date '1994-01-01'
  and l_receiptdate < date '1994-01-01' + interval '1' year
group by
   l_shipmode
order by
   l_shipmode;

--#################################
--# Report
--#################################

\set timing off;
INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q12', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;
