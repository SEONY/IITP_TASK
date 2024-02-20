--####################################################################
--# Small-Quantity-Order Revenue Query (Q17)
--####################################################################

--# This query determines how much average yearly revenue would be lost 
--# if orders were no longer filled for small quantities of certain parts.
--#  This may reduce overhead expenses by concentrating sales 
--# on larger shipments.

--###############################
--# Business Question
--###############################

--# The Small-Quantity-Order Revenue Query considers parts of 
--# a given brand and with a given container type and determines 
--# the average lineitem quantity of such parts ordered for 
--# all orders (past and pending) in the 7-year data-base. 
--# What would be the average yearly gross (undiscounted) loss 
--# in revenue if orders for these parts with a quantity of 
--# less than 20% of this average were no longer taken?

\explain plan
select
    ROUND( sum(l_extendedprice) / 7.0, 2) as avg_yearly
from
    lineitem,
    part
where
      p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
                     select
                         0.2 * avg(l_quantity)
                     from
                         lineitem
                     where
                         l_partkey = p_partkey
                   );

--###############################
--# Functional Query Definition
--###############################

\set linesize 400
\set timing on;

--# result: 1 rows
--#  avg_yearly 
--# ------------
--#   348406.05
\explain plan
select
    ROUND( sum(l_extendedprice) / 7.0, 2) as avg_yearly
from
    lineitem,
    part
where
      p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
                     select
                         0.2 * avg(l_quantity)
                     from
                         lineitem
                     where
                         l_partkey = p_partkey
                   );


--#################################
--# Report
--#################################

\set timing off;

INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q17', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;

