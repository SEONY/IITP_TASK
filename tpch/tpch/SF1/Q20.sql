--####################################################################
--# Potential Part Promotion Query (Q20)
--####################################################################

--# The Potential Part Promotion Query identifies suppliers 
--# in a particular nation having selected parts that may be 
--# can-didates for a promotional offer.

--###############################
--# Business Question
--###############################

--# The Potential Part Promotion query identifies suppliers 
--# who have an excess of a given part available; 
--# an excess is defined to be more than 50% of the parts like 
--# the given part that the supplier shipped in a given year 
--# for a given nation. 
--# Only parts whose names share a certain naming convention 
--# are considered.

\explain plan
select
    s_name,
    s_address
from
    supplier, 
    nation
where
    s_suppkey in (
                   select
                       ps_suppkey
                   from
                       partsupp
                   where
                       ps_partkey 
                       in (
                            select
                                p_partkey
                            from
                                part
                            where
                                p_name like 'forest%'
                          )
                   and ps_availqty 
                       > (
                           select
                               0.5 * sum(l_quantity)
                           from
                               lineitem
                           where
                                 l_partkey = ps_partkey
                             and l_suppkey = ps_suppkey
                             and l_shipdate >= date'1994-01-01'
                             and l_shipdate < date'1994-01-01' + interval '1' year
                         )
                )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
    s_name;

--###############################
--# Functional Query Definition
--###############################

\set linesize 400
\set timing on;

--# result: 186 rows
--#           s_name           |                s_address                 
--# ---------------------------+------------------------------------------
--#  Supplier#000000020        | iybAE,RmTymrZVYaFZva2SH,j
--#  Supplier#000000091        | YV45D7TkfdQanOOZ7q9QxkyGUapU1oOWU6q3
--#  .....
--#  .....
--#  .....
--#  Supplier#000009899        | 7XdpAHrzr1t,UQFZE
--#  Supplier#000009974        | 7wJ,J5DKcxSU4Kp1cQLpbcAvB5AsvKT
\explain plan
select
    s_name,
    s_address
from
    supplier, 
    nation
where
    s_suppkey in (
                   select
                       ps_suppkey
                   from
                       partsupp
                   where
                       ps_partkey 
                       in (
                            select
                                p_partkey
                            from
                                part
                            where
                                p_name like 'forest%'
                          )
                   and ps_availqty 
                       > (
                           select
                               0.5 * sum(l_quantity)
                           from
                               lineitem
                           where
                                 l_partkey = ps_partkey
                             and l_suppkey = ps_suppkey
                             and l_shipdate >= date'1994-01-01'
                             and l_shipdate < date'1994-01-01' + interval '1' year
                         )
                )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
    s_name;

--#################################
--# Report
--#################################

\set timing off;

INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q20', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;
