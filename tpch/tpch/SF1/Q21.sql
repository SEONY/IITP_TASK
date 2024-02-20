--####################################################################
--# Suppliers Who Kept Orders Waiting Query (Q21)
--####################################################################

--# This query identifies certain suppliers who were not able to 
--# ship required parts in a timely manner.

--###############################
--# Business Question
--###############################

--# The Suppliers Who Kept Orders Waiting query identifies suppliers, 
--# for a given nation, whose product was part of a multi-supplier 
--# order (with current status of 'F') where they were the only supplier 
--# who failed to meet the committed delivery date.

\explain plan
  select
      s_name,
      count(*) as numwait
  from
      supplier,
	  lineitem l1,
	  orders,
	  nation
  where
        s_suppkey = l1.l_suppkey
    and o_orderkey = l1.l_orderkey
	and o_orderstatus = 'F'
	and l1.l_receiptdate > l1.l_commitdate
	and exists (
                 select
                     *
                 from
                     lineitem l2
                 where
                       l2.l_orderkey = l1.l_orderkey
                   and l2.l_suppkey <> l1.l_suppkey
               )
    and not exists (
                     select
                         *
                     from
                         lineitem l3
                     where
                           l3.l_orderkey = l1.l_orderkey
                       and l3.l_suppkey <> l1.l_suppkey
                       and l3.l_receiptdate > l3.l_commitdate
                   )
    and s_nationkey = n_nationkey
    and n_name = 'SAUDI ARABIA'
  group by
      s_name
  order by
      numwait desc,
      s_name
  fetch 100;

--###############################
--# Functional Query Definition
--###############################

--# Return the first 100 selected rows.

\set linesize 400
\set timing on;

--# result: 100 rows
--#           s_name           | numwait 
--# ---------------------------+---------
--#  Supplier#000002829        |      20
--#  Supplier#000005808        |      18
--#  .....
--#  .....
--#  .....
--#  Supplier#000002357        |      12
--#  Supplier#000002483        |      12
\explain plan
  select
      s_name,
      count(*) as numwait
  from
      supplier,
	  lineitem l1,
	  orders,
	  nation
  where
        s_suppkey = l1.l_suppkey
    and o_orderkey = l1.l_orderkey
	and o_orderstatus = 'F'
	and l1.l_receiptdate > l1.l_commitdate
	and exists (
                 select
                     *
                 from
                     lineitem l2
                 where
                       l2.l_orderkey = l1.l_orderkey
                   and l2.l_suppkey <> l1.l_suppkey
               )
    and not exists (
                     select
                         *
                     from
                         lineitem l3
                     where
                           l3.l_orderkey = l1.l_orderkey
                       and l3.l_suppkey <> l1.l_suppkey
                       and l3.l_receiptdate > l3.l_commitdate
                   )
    and s_nationkey = n_nationkey
    and n_name = 'SAUDI ARABIA'
  group by
      s_name
  order by
      numwait desc,
      s_name
  fetch 100;

--#################################
--# Report
--#################################

\set timing off;

INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q21', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;


