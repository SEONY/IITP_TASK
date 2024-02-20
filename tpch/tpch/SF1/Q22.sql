--####################################################################
--# Global Sales Opportunity Query (Q22)
--####################################################################

--# The Global Sales Opportunity Query identifies geographies 
--# where there are customers who may be likely to make a purchase.

--###############################
--# Business Question
--###############################

--# This query counts how many customers within a specific range 
--# of country codes have not placed orders for 7 years but 
--# who have a greater than average “positive” account balance. 
--# It also reflects the magnitude of that balance. 
--# Country code is defined as the first two characters of c_phone.

\explain plan
select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from (
       select
           substr(c_phone, 1, 2) as cntrycode,
           c_acctbal
       from
           customer
       where
             substr(c_phone, 1, 2) in
                ('13','31','23','29','30','18','17')
         and c_acctbal > (
                           select
                               avg(c_acctbal)
                           from
                               customer
                           where
                                 c_acctbal > 0.00
                             and substr(c_phone, 1, 2) in
                                ('13','31','23','29','30','18','17')
                         )
         and not exists (
                          select
                              *
                          from
                              orders
                          where
                              o_custkey = c_custkey
                        )
     ) custsale
group by
    cntrycode
order by
    cntrycode;

--###############################
--# Functional Query Definition
--###############################

\set linesize 400
\set timing on;

--# result: 7 rows
--#  cntrycode | numcust | totacctbal 
--# -----------+---------+------------
--#  13        |     888 | 6737713.99
--#  17        |     861 | 6460573.72
--#  18        |     964 | 7236687.40
--#  23        |     892 | 6701457.95
--#  29        |     948 | 7158866.63
--#  30        |     909 | 6808436.13
--#  31        |     922 | 6806670.18

\explain plan
select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from (
       select
           substr(c_phone, 1, 2) as cntrycode,
           c_acctbal
       from
           customer
       where
             substr(c_phone, 1, 2) in
                ('13','31','23','29','30','18','17')
         and c_acctbal > (
                           select
                               avg(c_acctbal)
                           from
                               customer
                           where
                                 c_acctbal > 0.00
                             and substr(c_phone, 1, 2) in
                                ('13','31','23','29','30','18','17')
                         )
         and not exists (
                          select
                              *
                          from
                              orders
                          where
                              o_custkey = c_custkey
                        )
     ) custsale
group by
    cntrycode
order by
    cntrycode;

--#################################
--# Report
--#################################

\set timing off;

INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q22', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;

