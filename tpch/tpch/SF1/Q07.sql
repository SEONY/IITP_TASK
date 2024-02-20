--####################################################################
--# Volume Shipping Query (Q7)
--####################################################################

--# This query determines the value of goods shipped between 
--# certain nations to help in the re-negotiation of shipping contracts.

--###############################
--# Business Question
--###############################

--# The Volume Shipping Query finds, for two given nations, 
--# the gross discounted revenues derived from lineitems 
--# in which parts were shipped from a supplier in either nation 
--# to a customer in the other nation during 1995 and 1996. 
--# The query lists the supplier nation, the customer nation, 
--# the year, and the revenue from shipments that took place in that year.
--# The query orders the answer by Supplier nation, Customer nation, 
--# and year (all ascending).

\explain plan
select
    supp_nation,
    cust_nation,
    l_year, 
    ROUND( sum(volume), 2 ) as revenue
from (
       select
           n1.n_name as supp_nation,
           n2.n_name as cust_nation,
           extract(year from l_shipdate) as l_year,
           l_extendedprice * (1 - l_discount) as volume
       from
           supplier,
           lineitem,
           orders,
           customer,
           nation n1,
           nation n2
       where
             s_suppkey = l_suppkey
         and o_orderkey = l_orderkey
         and c_custkey = o_custkey
         and s_nationkey = n1.n_nationkey
         and c_nationkey = n2.n_nationkey
         and (
               (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
               or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
             )
         and l_shipdate between date '1995-01-01' and date '1996-12-31'
     ) shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year;

--###############################
--# Functional Query Definition
--###############################

\set linesize 400
\set timing on;

--# result: 4 rows
--#         supp_nation        |        cust_nation        | l_year |   revenue   
--# ---------------------------+---------------------------+--------+-------------
--#  FRANCE                    | GERMANY                   |   1995 | 54639732.73
--#  FRANCE                    | GERMANY                   |   1996 | 54633083.31
--#  GERMANY                   | FRANCE                    |   1995 | 52531746.67
--#  GERMANY                   | FRANCE                    |   1996 | 52520549.02
\explain plan
select
    supp_nation,
    cust_nation,
    l_year, 
    ROUND( sum(volume), 2 ) as revenue
from (
       select
           n1.n_name as supp_nation,
           n2.n_name as cust_nation,
           extract(year from l_shipdate) as l_year,
           l_extendedprice * (1 - l_discount) as volume
       from
           supplier,
           lineitem,
           orders,
           customer,
           nation n1,
           nation n2
       where
             s_suppkey = l_suppkey
         and o_orderkey = l_orderkey
         and c_custkey = o_custkey
         and s_nationkey = n1.n_nationkey
         and c_nationkey = n2.n_nationkey
         and (
               (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
               or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
             )
         and l_shipdate between date '1995-01-01' and date '1996-12-31'
     ) shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year;

--#################################
--# Report
--#################################

\set timing off;

INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q07', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;
