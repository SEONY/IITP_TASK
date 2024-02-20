--####################################################################
--# Parts/Supplier Relationship Query (Q16)
--####################################################################

--# This query finds out how many suppliers can supply parts 
--# with given attributes. 
--# It might be used, for example, to determine whether 
--# there is a sufficient number of suppliers for heavily ordered parts.

--###############################
--# Business Question
--###############################

--# The Parts/Supplier Relationship Query counts the number of 
--# suppliers who can supply parts that satisfy a particular 
--# customer's requirements. 
--# The customer is interested in parts of eight different sizes 
--# as long as they are not of a given type, not of a given brand, 
--# and not from a supplier who has had complaints registered 
--# at the Better Business Bureau. 
--# Results must be presented in descending count and ascending brand, 
--# type, and size.

\explain plan
select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    partsupp,
    part
where
      p_partkey = ps_partkey
  and p_brand <> 'Brand#45'
  and p_type not like 'MEDIUM POLISHED%'
  and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
  and ps_suppkey not in (
                          select
                              s_suppkey
                          from
                              supplier
                          where
                              s_comment like '%Customer%Complaints%'
                        )
group by
    p_brand,
    p_type,
    p_size
order by
      supplier_cnt desc,
      p_brand,
      p_type,
      p_size;

--###############################
--# Functional Query Definition
--###############################

\set linesize 400
\set timing on;

--# result: 18314 rows
--#   p_brand   |          p_type           | p_size | supplier_cnt 
--# ------------+---------------------------+--------+--------------
--#  Brand#41   | MEDIUM BRUSHED TIN        |      3 |           28
--#  Brand#54   | STANDARD BRUSHED COPPER   |     14 |           27
--#  .....
--#  .....
--#  .....
--#  Brand#55   | PROMO PLATED BRASS        |     19 |            3
--#  Brand#55   | STANDARD PLATED TIN       |     49 |            3
\explain plan
select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    partsupp,
    part
where
      p_partkey = ps_partkey
  and p_brand <> 'Brand#45'
  and p_type not like 'MEDIUM POLISHED%'
  and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
  and ps_suppkey not in (
                          select
                              s_suppkey
                          from
                              supplier
                          where
                              s_comment like '%Customer%Complaints%'
                        )
group by
    p_brand,
    p_type,
    p_size
order by
      supplier_cnt desc,
      p_brand,
      p_type,
      p_size;

\set timing off;

INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q16_DISPLAY', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;



--###############################
--# w/o display
--# display 성능이 jitter 가 되고 있어 별도 측정함.
--###############################

\set linesize 400
\set result off
\set timing on;

--# result: 18314 rows
--#   p_brand   |          p_type           | p_size | supplier_cnt 
--# ------------+---------------------------+--------+--------------
--#  Brand#41   | MEDIUM BRUSHED TIN        |      3 |           28
--#  Brand#54   | STANDARD BRUSHED COPPER   |     14 |           27
--#  .....
--#  .....
--#  .....
--#  Brand#55   | PROMO PLATED BRASS        |     19 |            3
--#  Brand#55   | STANDARD PLATED TIN       |     49 |            3
\explain plan
select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    partsupp,
    part
where
      p_partkey = ps_partkey
  and p_brand <> 'Brand#45'
  and p_type not like 'MEDIUM POLISHED%'
  and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
  and ps_suppkey not in (
                          select
                              s_suppkey
                          from
                              supplier
                          where
                              s_comment like '%Customer%Complaints%'
                        )
group by
    p_brand,
    p_type,
    p_size
order by
      supplier_cnt desc,
      p_brand,
      p_type,
      p_size;

\set result on
\set timing off;

INSERT INTO TPCH_SF1_REPORT 
       VALUES ( 'SF1_Q16_NO_DISPLAY', (:VAR_ELAPSED_TIME__ / 1000), TRUNC( 60 * 60 * 1000 / :VAR_ELAPSED_TIME__, 0 ) );
COMMIT;
