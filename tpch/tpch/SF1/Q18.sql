--####################################################################
--# Large Volume Customer Query (Q18)
--####################################################################

--# The Large Volume Customer Query ranks customers based on 
--# their having placed a large quantity order. 
--# Large quantity orders are defined as those orders 
--# whose total quantity is above a certain level.

--###############################
--# Business Question
--###############################

--# The Large Volume Customer Query finds a list of 
--# the top 100 customers who have ever placed large quantity orders. 
--# The query lists the customer name, customer key, the order key, 
--# date and total price and the quantity for the order.

\explain plan
  select
      c_name,
	  c_custkey,
	  o_orderkey,
	  o_orderdate,
	  o_totalprice,
	  sum(l_quantity)
  from
      customer,
	  orders,
	  lineitem
  where
      o_orderkey in (
                      select
                          l_orderkey
                      from
                          lineitem
                      group by
                          l_orderkey 
                      having
                          sum(l_quantity) > 300
                    )
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey
  group by
      c_name,
	  c_custkey,
	  o_orderkey,
	  o_orderdate,
	  o_totalprice
  order by
      o_totalprice desc,
      o_orderdate
  fetch 100;

--###############################
--# Functional Query Definition
--###############################

--# Return the first 100 selected rows

\set linesize 400
\set timing on;

--# result: 57 rows
--#        c_name       | c_custkey | o_orderkey | o_orderdate | o_totalprice |  sum   
--# --------------------+-----------+------------+-------------+--------------+--------
--#  Customer#000128120 |    128120 |    4722021 | 1994-04-07  |    544089.09 | 323.00
--#  Customer#000144617 |    144617 |    3043270 | 1997-02-12  |    530604.44 | 317.00
--#  .....
--#  .....
--#  .....
--#  Customer#000082441 |     82441 |     857959 | 1994-02-07  |    382579.74 | 305.00
--#  Customer#000088703 |     88703 |    2995076 | 1994-01-30  |    363812.12 | 302.00

\explain plan
  select
      c_name,
	  c_custkey,
	  o_orderkey,
	  o_orderdate,
	  o_totalprice,
	  sum(l_quantity)
  from
      customer,
	  orders,
	  lineitem
  where
      o_orderkey in (
                      select
                          l_orderkey
                      from
                          lineitem
                      group by
                          l_orderkey 
                      having
                          sum(l_quantity) > 300
                    )
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey
  group by
      c_name,
	  c_custkey,
	  o_orderkey,
	  o_orderdate,
	  o_totalprice
  order by
      o_totalprice desc,
      o_orderdate
  fetch 100;

--#################################
--# Report
--#################################

\set timing off;

INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q18', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;

