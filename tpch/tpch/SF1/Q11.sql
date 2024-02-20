--####################################################################
--# Important Stock Identification Query (Q11)
--####################################################################

--# This query finds the most important subset of suppliers' stock 
--# in a given nation.

--###############################
--# Business Question
--###############################

--# The Important Stock Identification Query finds, from scanning 
--# the available stock of suppliers in a given nation, all the parts 
--# that represent a significant percentage of the total value of all 
--# available parts. 
--# The query displays the part number and the value of those parts 
--# in descending order of value.

\explain plan
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
      ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'GERMANY'
group by
    ps_partkey having
    sum(ps_supplycost * ps_availqty) 
    > (
        select
            sum(ps_supplycost * ps_availqty) * 0.0001
        from
            partsupp,
            supplier,
            nation
        where
              ps_suppkey = s_suppkey
          and s_nationkey = n_nationkey
          and n_name = 'GERMANY'
      )
order by
    value desc;

--###############################
--# Functional Query Definition
--###############################

\set linesize 400
\set timing on;

--# result: 1048 rows
--#  ps_partkey |    value    
--# ------------+-------------
--#      129760 | 17538456.86
--#      166726 | 16503353.92
--#  .....
--#  .....
--#  .....
--#       72073 |  7877736.11
--#        5182 |  7874521.73
\explain plan
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
      ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'GERMANY'
group by
    ps_partkey having
    sum(ps_supplycost * ps_availqty) 
    > (
        select
            sum(ps_supplycost * ps_availqty) * 0.0001
        from
            partsupp,
            supplier,
            nation
        where
              ps_suppkey = s_suppkey
          and s_nationkey = n_nationkey
          and n_name = 'GERMANY'
      )
order by
    value desc;

--#################################
--# Report
--#################################

\set timing off;

INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q11', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;

