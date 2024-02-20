
--####################################################################
--# Minimum Cost Supplier Query (Q2)
--####################################################################

--# This query finds which supplier should be selected 
--# to place an order for a given part in a given region.

--###############################
--# Business Question
--###############################

--# The Minimum Cost Supplier Query finds, in a given region, 
--# for each part of a certain type and size, the supplier 
--# who can supply it at minimum cost. 
--# If several suppliers in that region offer the desired part type 
--# and size at the same (minimum) cost, the query lists the parts 
--# from suppliers with the 100 highest account balances. 
--# For each supplier, the query lists the supplier's account balance, 
--# name and nation; the part's number and manufacturer; 
--# the supplier's address, phone number and comment information.

\explain plan
  select
      s_acctbal,
      s_name,
      n_name,
	  p_partkey,
	  p_mfgr,
	  s_address,
	  s_phone,
	  s_comment
  from
      part,
	  supplier,
	  partsupp,
	  nation,
	  region
  where
        p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
	and p_size = 15
	and p_type like '%BRASS'
    and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'EUROPE'
	and ps_supplycost = (
                           select
                                  min(ps_supplycost)
                           from
                                  partsupp, supplier,
                                  nation, region
                           where
                                  p_partkey = ps_partkey
                              and s_suppkey = ps_suppkey
							  and s_nationkey = n_nationkey
							  and n_regionkey = r_regionkey
							  and r_name = 'EUROPE'
                        )
  order by
      s_acctbal desc,
	  n_name,
	  s_name,
	  p_partkey
  fetch 100;

--###############################
--# Functional Query Definition
--###############################

--# Return the first 100 selected rows

\set linesize 400
\set timing on;

--# result: 100 rows
--# s_acctbal |          s_name           |          n_name           | p_partkey |          p_mfgr           |                s_address                 |     s_phone     |                                              s_comment                                               
--# -----------+---------------------------+---------------------------+-----------+---------------------------+------------------------------------------+-----------------+-----------------------------------------------------------------# ---------------------------------------
--#    9938.53 | Supplier#000005359        | UNITED KINGDOM            |    185358 | Manufacturer#4            | QKuHYh,vZGiwu2FWEJoLDx04                 | 33-429-790-6131 | uriously regular requests hag
--#    9937.84 | Supplier#000005969        | ROMANIA                   |    108438 | Manufacturer#1            | ANDENSOSmk,miq23Xfb5RWt6dvUcvt6Qa        | 29-520-692-3537 | efully express instructions. regular requests against the slyly fin
--# .....
--# .....
--# .....
--#    7850.66 | Supplier#000001518        | UNITED KINGDOM            |     86501 | Manufacturer#1            | ONda3YJiHKJOC                            | 33-730-383-3892 | ifts haggle fluffily pending pai
--#    7843.52 | Supplier#000006683        | FRANCE                    |     11680 | Manufacturer#4            | 2Z0JGkiv01Y00oCFwUGfviIbhzCdy            | 16-464-517-8943 |  express, final pinto beans x-ray slyly asymptotes. unusual, unusual

\explain plan
  select
      s_acctbal,
      s_name,
      n_name,
	  p_partkey,
	  p_mfgr,
	  s_address,
	  s_phone,
	  s_comment
  from
      part,
	  supplier,
	  partsupp,
	  nation,
	  region
  where
        p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
	and p_size = 15
	and p_type like '%BRASS'
    and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'EUROPE'
	and ps_supplycost = (
                           select
                                  min(ps_supplycost)
                           from
                                  partsupp, supplier,
                                  nation, region
                           where
                                  p_partkey = ps_partkey
                              and s_suppkey = ps_suppkey
							  and s_nationkey = n_nationkey
							  and n_regionkey = r_regionkey
							  and r_name = 'EUROPE'
                        )
  order by
      s_acctbal desc,
	  n_name,
	  s_name,
	  p_partkey
  fetch 100;

--#################################
--# Report
--#################################

\set timing off;

INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q02', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;
