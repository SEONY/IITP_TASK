--####################################################################
--# Promotion Effect Query (Q14)
--####################################################################

--# This query monitors the market response to a promotion such as 
--# TV advertisements or a special campaign.

--###############################
--# Business Question
--###############################

--# The Promotion Effect Query determines what percentage of 
--# the revenue in a given year and month was derived from 
--# promotional parts. 
--# The query considers only parts actually shipped in 
--# that month and gives the percentage. 
--# Revenue is defined as (l_extendedprice * (1-l_discount)).

\explain plan
select
    ROUND( 100.00 * sum( case
                      when p_type like 'PROMO%'
                      then l_extendedprice*(1-l_discount)
                      else 0
                  end ) 
           / sum(l_extendedprice * (1 - l_discount)), 2 ) 
    as promo_revenue
from
    lineitem,
    part
where
      l_partkey = p_partkey
  and l_shipdate >= date '1995-09-01'
  and l_shipdate < date '1995-09-01' + interval '1' month;

--###############################
--# Functional Query Definition
--###############################

\set linesize 400
\set timing on;

--# result: 1 rows
--#  promo_revenue 
--# ---------------
--#          16.38
\explain plan
select
    ROUND( 100.00 * sum( case
                      when p_type like 'PROMO%'
                      then l_extendedprice*(1-l_discount)
                      else 0
                  end ) 
           / sum(l_extendedprice * (1 - l_discount)), 2 ) 
    as promo_revenue
from
    lineitem,
    part
where
      l_partkey = p_partkey
  and l_shipdate >= date '1995-09-01'
  and l_shipdate < date '1995-09-01' + interval '1' month;

--#################################
--# Report
--#################################

\set timing off;
INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q14', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;
