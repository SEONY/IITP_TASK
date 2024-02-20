--####################################################################
--# Forecasting Revenue Change Query (Q6)
--####################################################################

--# This query quantifies the amount of revenue increase 
--# that would have resulted from eliminating certain company-wide 
--# discounts in a given percentage range in a given year. 
--# Asking this type of "what if" query can be used to look for 
--# ways to increase revenues.

--###############################
--# Business Question
--###############################

--# The Forecasting Revenue Change Query considers all the lineitems 
--# shipped in a given year with discounts between DISCOUNT-0.01 
--# and DISCOUNT+0.01. 
--# The query lists the amount by which the total revenue would 
--# have increased if these discounts had been eliminated 
--# for lineitems with l_quantity less than quantity. 
--# Note that the potential revenue increase is equal to the sum 
--# of [l_extendedprice * l_discount] for all lineitems 
--# with discounts and quantities in the qualifying range.

\explain plan
select
    ROUND( sum(l_extendedprice*l_discount), 2) as revenue
from
    lineitem
where
      l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between 0.06 - 0.01 and 0.06 + 0.01
  and l_quantity < 24;

--###############################
--# Functional Query Definition
--###############################

\set linesize 400
\set timing on;

--# result: 1 rows
--#   revenue    
--# --------------
--#  123141078.23
\explain plan
select
    ROUND( sum(l_extendedprice*l_discount), 2) as revenue
from
    lineitem
where
      l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between 0.06 - 0.01 and 0.06 + 0.01
  and l_quantity < 24;

--#################################
--# Report
--#################################

\set timing off;
INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q06', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;
