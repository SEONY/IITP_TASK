--####################################################################
--# Pricing Summary Report Query (Q1)
--####################################################################

--# This query reports the amount of business 
--# that was billed, shipped, and returned.

--###############################
--# Business Question
--###############################

--# The Pricing Summary Report Query provides a summary pricing report 
--# for all lineitems shipped as of a given date. 
--# The date is within 60 - 120 days of the greatest ship date 
--# contained in the database. 
--# The query lists totals for extended price, discounted extended price,
--#  discounted extended price plus tax, average quantity, 
--# average extended price, and average discount. 
--# These aggregates are grouped by RETURNFLAG and LINESTATUS, 
--# and listed in ascending order of RETURNFLAG and LINESTATUS. 
--# A count of the number of lineitems in each group is included.

\explain plan
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    ROUND(sum(l_extendedprice*(1-l_discount)), 2) as sum_disc_price,
    ROUND(sum(l_extendedprice*(1-l_discount)*(1+l_tax)), 2) as sum_charge,
    ROUND(avg(l_quantity),2) as avg_qty,
    ROUND(avg(l_extendedprice),2) as avg_price,
    ROUND(avg(l_discount),2) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-12-01' - interval '90' day
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;

--###############################
--# Functional Query Definition
--###############################

\set linesize 400
\set timing on;

--# result: 4 rows
--#  l_returnflag | l_linestatus |   sum_qty   | sum_base_price  | sum_disc_price  |   sum_charge    | avg_qty | avg_price | avg_disc | count_order 
--# --------------+--------------+-------------+-----------------+-----------------+-----------------+---------+-----------+----------+-------------
--#  A            | F            | 37734107.00 |  56586554400.73 |  53758257134.87 |  55909065222.83 |   25.52 |  38273.13 |     0.05 |     1478493
--#  N            | F            |   991417.00 |   1487504710.38 |   1413082168.05 |   1469649223.19 |   25.52 |  38284.47 |     0.05 |       38854
--#  N            | O            | 74476040.00 | 111701729697.74 | 106118230307.61 | 110367043872.50 |   25.50 |  38249.12 |     0.05 |     2920374
--#  R            | F            | 37719753.00 |  56568041380.90 |  53741292684.60 |  55889619119.83 |   25.51 |  38250.85 |     0.05 |     1478870

\explain plan
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    ROUND(sum(l_extendedprice*(1-l_discount)), 2) as sum_disc_price,
    ROUND(sum(l_extendedprice*(1-l_discount)*(1+l_tax)), 2) as sum_charge,
    ROUND(avg(l_quantity),2) as avg_qty,
    ROUND(avg(l_extendedprice),2) as avg_price,
    ROUND(avg(l_discount),2) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-12-01' - interval '90' day
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;

--#################################
--# Report
--#################################

\set timing off;
INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q01', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;
