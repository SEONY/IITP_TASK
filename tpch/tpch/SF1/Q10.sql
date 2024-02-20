--####################################################################
--# Returned Item Reporting Query (Q10)
--####################################################################

--# The query identifies customers who might be having problems 
--# with the parts that are shipped to them.

--###############################
--# Business question
--###############################

--# The Returned Item Reporting Query finds the top 20 customers, 
--# in terms of their effect on lost revenue for a given quarter, 
--# who have returned parts. The query considers only parts 
--# that were ordered in the specified quarter. 
--# The query lists the customer's name, address, nation, phone number, 
--# account balance, comment information and revenue lost. 
--# The customers are listed in descending order of lost revenue. 
--# Revenue lost is defined as sum(l_extendedprice*(1-l_discount)) 
--# for all qualifying lineitems.

\explain plan
  select
      c_custkey,
	  c_name,
	  ROUND( sum(l_extendedprice * (1 - l_discount)), 2) as revenue,
	  c_acctbal,
	  n_name,
	  c_address,
	  c_phone,
	  c_comment
  from
      customer,
	  orders,
	  lineitem,
	  nation
 where
        c_custkey = o_custkey
    and l_orderkey = o_orderkey
	and o_orderdate >= date '1993-10-01'
	and o_orderdate < date '1993-10-01' + interval '3' month
	and l_returnflag = 'R'
    and c_nationkey = n_nationkey
  group by
      c_custkey,
	  c_name,
	  c_acctbal,
	  c_phone,
	  n_name,
	  c_address,
	  c_comment
  order by
        revenue desc
  fetch 20;

--###############################
--# Functional Query Definition
--###############################

--# Return the first 20 selected rows

\set linesize 400
\set timing on;

--# result: 20 rows
--#  c_custkey |       c_name       |  revenue  | c_acctbal |          n_name           |                c_address                 |     c_phone     |                                                    c_comment                                                     
--# -----------+--------------------+-----------+-----------+---------------------------+------------------------------------------+-----------------+------------------------------------------------------------------------------------------------------------------
--#      57040 | Customer#000057040 | 734235.25 |    632.87 | JAPAN                     | Eioyzjf4pp                               | 22-895-641-3466 | sits. slyly regular requests sleep alongside of the regular inst
--#     143347 | Customer#000143347 | 721002.69 |   2557.47 | EGYPT                     | 1aReFYv,Kw4                              | 14-742-935-3718 | ggle carefully enticing requests. final deposits use bold, bold pinto beans. ironic, idle re
--#  .....
--#  .....
--#  .....
--#  .....
--#  .....
--#      52528 | Customer#000052528 | 556397.35 |    551.79 | ARGENTINA                 | NFztyTOR10UOJ                            | 11-208-192-3205 |  deposits hinder. blithely pending asymptotes breach slyly regular re
--#      23431 | Customer#000023431 | 554269.54 |   3381.86 | ROMANIA                   | HgiV0phqhaIa9aydNoIlb                    | 29-915-458-2654 | nusual, even instructions: furiously stealthy n

\explain plan
  select
      c_custkey,
	  c_name,
	  ROUND( sum(l_extendedprice * (1 - l_discount)), 2) as revenue,
	  c_acctbal,
	  n_name,
	  c_address,
	  c_phone,
	  c_comment
  from
      customer,
	  orders,
	  lineitem,
	  nation
 where
        c_custkey = o_custkey
    and l_orderkey = o_orderkey
	and o_orderdate >= date '1993-10-01'
	and o_orderdate < date '1993-10-01' + interval '3' month
	and l_returnflag = 'R'
    and c_nationkey = n_nationkey
  group by
      c_custkey,
	  c_name,
	  c_acctbal,
	  c_phone,
	  n_name,
	  c_address,
	  c_comment
  order by
        revenue desc
  fetch 20;

--#################################
--# Report
--#################################

\set timing off;
INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q10', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;
