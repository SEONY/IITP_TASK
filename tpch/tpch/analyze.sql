--#########################################
--# ANALYZE SYSTEM
--#########################################

--# result: success
--# regression test 를 위한 분석은 장비마다 DIFF 를 유발할 수 있음.
--# ANALYZE SYSTEM COMPUTE STATISTICS;
--# COMMIT;

--######################################################################
--# Analyze Table
--######################################################################

\set linesize 400
\set pagesize 400

--\set timing on

--# result: success
ANALYZE TABLE region;
COMMIT;

--# result: success
ANALYZE TABLE nation;
COMMIT;

--# result: success
ANALYZE TABLE supplier;
COMMIT;

--# result: success
ANALYZE TABLE customer;
COMMIT;

--# result: success
ANALYZE TABLE part;
COMMIT;

--# result: success
ANALYZE TABLE partsupp;
COMMIT;

--# result: success
ANALYZE TABLE orders;
COMMIT;

--# result: success
ANALYZE TABLE lineitem;
COMMIT;


--\set timing off

--# result: success
ANALYZE TABLE region EXPORT REPLACE 'region.stat';
COMMIT;

--# result: success
ANALYZE TABLE nation EXPORT REPLACE 'nation.stat';
COMMIT;

--# result: success
ANALYZE TABLE supplier EXPORT REPLACE 'supplier.stat';
COMMIT;

--# result: success
ANALYZE TABLE customer EXPORT REPLACE 'customer.stat';
COMMIT;

--# result: success
ANALYZE TABLE part EXPORT REPLACE 'part.stat';
COMMIT;

--# result: success
ANALYZE TABLE partsupp EXPORT REPLACE 'partsupp.stat';
COMMIT;

--# result: success
ANALYZE TABLE orders EXPORT REPLACE 'orders.stat';
COMMIT;

--# result: success
ANALYZE TABLE lineitem EXPORT REPLACE 'lineitem.stat';
COMMIT;


--# result: success
ANALYZE TABLE region DELETE STATISTICS;
COMMIT;

--# result: success
ANALYZE TABLE nation DELETE STATISTICS;
COMMIT;

--# result: success
ANALYZE TABLE supplier DELETE STATISTICS;
COMMIT;

--# result: success
ANALYZE TABLE customer DELETE STATISTICS;
COMMIT;

--# result: success
ANALYZE TABLE part DELETE STATISTICS;
COMMIT;

--# result: success
ANALYZE TABLE partsupp DELETE STATISTICS;
COMMIT;

--# result: success
ANALYZE TABLE orders DELETE STATISTICS;
COMMIT;

--# result: success
ANALYZE TABLE lineitem DELETE STATISTICS;
COMMIT;


--# result: success
ANALYZE TABLE region IMPORT 'region.stat';
COMMIT;

--# result: success
ANALYZE TABLE nation IMPORT 'nation.stat';
COMMIT;

--# result: success
ANALYZE TABLE supplier IMPORT 'supplier.stat';
COMMIT;

--# result: success
ANALYZE TABLE customer IMPORT 'customer.stat';
COMMIT;

--# result: success
ANALYZE TABLE part IMPORT 'part.stat';
COMMIT;

--# result: success
ANALYZE TABLE partsupp IMPORT 'partsupp.stat';
COMMIT;

--# result: success
ANALYZE TABLE orders IMPORT 'orders.stat';
COMMIT;

--# result: success
ANALYZE TABLE lineitem IMPORT 'lineitem.stat';
COMMIT;

--##############################################
--# REGION
--##############################################

--# result: 1 row
SELECT 
       TABLE_NAME
     , NUM_ROWS
  FROM
       DICTIONARY_SCHEMA.USER_TABLES
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'REGION'
;

--# result: n rows
SELECT DISTINCT
       TABLE_NAME
     , GROUP_NAME
     , NUM_ROWS
  FROM
       DICTIONARY_SCHEMA.USER_TAB_PLACE
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'REGION'
 ORDER BY 1, 2
;

--# result: n rows
SELECT 
       TABLE_NAME
     , COLUMN_NAME
     , NUM_DISTINCT
     , AVG_COL_LEN
     , NUM_NULLS
     , LOW_VALUE
     , HIGH_VALUE
  FROM
       DICTIONARY_SCHEMA.USER_TAB_COLUMNS
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'REGION'
;


--# result: n rows
SELECT 
       TABLE_NAME
     , INDEX_NAME
     , DISTINCT_KEYS
  FROM
       DICTIONARY_SCHEMA.USER_INDEXES
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'REGION'
;


--##############################################
--# NATION
--##############################################

--# result: 1 row
SELECT 
       TABLE_NAME
     , NUM_ROWS
  FROM
       DICTIONARY_SCHEMA.USER_TABLES
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'NATION'
;

--# result: n rows
SELECT DISTINCT
       TABLE_NAME
     , GROUP_NAME
     , NUM_ROWS
  FROM
       DICTIONARY_SCHEMA.USER_TAB_PLACE
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'NATION'
 ORDER BY 1, 2
;

--# result: n rows
SELECT 
       TABLE_NAME
     , COLUMN_NAME
     , NUM_DISTINCT
     , AVG_COL_LEN
     , NUM_NULLS
     , LOW_VALUE
     , HIGH_VALUE
  FROM
       DICTIONARY_SCHEMA.USER_TAB_COLUMNS
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'NATION'
;

--# result: n rows
SELECT 
       TABLE_NAME
     , INDEX_NAME
     , DISTINCT_KEYS
  FROM
       DICTIONARY_SCHEMA.USER_INDEXES
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'NATION'
;


--##############################################
--# SUPPLIER
--##############################################

--# result: 1 row
SELECT 
       TABLE_NAME
     , NUM_ROWS
  FROM
       DICTIONARY_SCHEMA.USER_TABLES
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'SUPPLIER'
;

--# result: n rows
SELECT DISTINCT
       TABLE_NAME
     , GROUP_NAME
     , NUM_ROWS
  FROM
       DICTIONARY_SCHEMA.USER_TAB_PLACE
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'SUPPLIER'
 ORDER BY 1, 2
;

--# result: n rows
SELECT 
       TABLE_NAME
     , COLUMN_NAME
     , NUM_DISTINCT
     , AVG_COL_LEN
     , NUM_NULLS
     , LOW_VALUE
     , HIGH_VALUE
  FROM
       DICTIONARY_SCHEMA.USER_TAB_COLUMNS
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'SUPPLIER'
;


--# result: n rows
SELECT 
       TABLE_NAME
     , INDEX_NAME
     , DISTINCT_KEYS
  FROM
       DICTIONARY_SCHEMA.USER_INDEXES
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'SUPPLIER'
;


--##############################################
--# CUSTOMER
--##############################################

--# result: 1 row
SELECT 
       TABLE_NAME
     , NUM_ROWS
  FROM
       DICTIONARY_SCHEMA.USER_TABLES
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'CUSTOMER'
;

--# result: n rows
SELECT DISTINCT
       TABLE_NAME
     , GROUP_NAME
     , NUM_ROWS
  FROM
       DICTIONARY_SCHEMA.USER_TAB_PLACE
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'CUSTOMER'
 ORDER BY 1, 2
;

--# result: n rows
SELECT 
       TABLE_NAME
     , COLUMN_NAME
     , NUM_DISTINCT
     , AVG_COL_LEN
     , NUM_NULLS
     , LOW_VALUE
     , HIGH_VALUE
  FROM
       DICTIONARY_SCHEMA.USER_TAB_COLUMNS
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'CUSTOMER'
;


--# result: n rows
SELECT 
       TABLE_NAME
     , INDEX_NAME
     , DISTINCT_KEYS
  FROM
       DICTIONARY_SCHEMA.USER_INDEXES
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'CUSTOMER'
;


--##############################################
--# PART
--##############################################

--# result: 1 row
SELECT 
       TABLE_NAME
     , NUM_ROWS
  FROM
       DICTIONARY_SCHEMA.USER_TABLES
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'PART'
;

--# result: n rows
SELECT DISTINCT
       TABLE_NAME
     , GROUP_NAME
     , NUM_ROWS
  FROM
       DICTIONARY_SCHEMA.USER_TAB_PLACE
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'PART'
 ORDER BY 1, 2
;

--# result: n rows
SELECT 
       TABLE_NAME
     , COLUMN_NAME
     , NUM_DISTINCT
     , AVG_COL_LEN
     , NUM_NULLS
     , LOW_VALUE
     , HIGH_VALUE
  FROM
       DICTIONARY_SCHEMA.USER_TAB_COLUMNS
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'PART'
;


--# result: n rows
SELECT 
       TABLE_NAME
     , INDEX_NAME
     , DISTINCT_KEYS
  FROM
       DICTIONARY_SCHEMA.USER_INDEXES
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'PART'
;


--##############################################
--# PARTSUPP
--##############################################

--# result: 1 row
SELECT 
       TABLE_NAME
     , NUM_ROWS
  FROM
       DICTIONARY_SCHEMA.USER_TABLES
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'PARTSUPP'
;

--# result: n rows
SELECT DISTINCT
       TABLE_NAME
     , GROUP_NAME
     , NUM_ROWS
  FROM
       DICTIONARY_SCHEMA.USER_TAB_PLACE
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'PARTSUPP'
 ORDER BY 1, 2
;

--# result: n rows
SELECT 
       TABLE_NAME
     , COLUMN_NAME
     , NUM_DISTINCT
     , AVG_COL_LEN
     , NUM_NULLS
     , LOW_VALUE
     , HIGH_VALUE
  FROM
       DICTIONARY_SCHEMA.USER_TAB_COLUMNS
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'PARTSUPP'
;


--# result: n rows
SELECT 
       TABLE_NAME
     , INDEX_NAME
     , DISTINCT_KEYS
  FROM
       DICTIONARY_SCHEMA.USER_INDEXES
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'PARTSUPP'
;


--##############################################
--# ORDERS
--##############################################

--# result: 1 row
SELECT 
       TABLE_NAME
     , NUM_ROWS
  FROM
       DICTIONARY_SCHEMA.USER_TABLES
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'ORDERS'
;

--# result: n rows
SELECT DISTINCT
       TABLE_NAME
     , GROUP_NAME
     , NUM_ROWS
  FROM
       DICTIONARY_SCHEMA.USER_TAB_PLACE
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'ORDERS'
 ORDER BY 1, 2
;

--# result: n rows
SELECT 
       TABLE_NAME
     , COLUMN_NAME
     , NUM_DISTINCT
     , AVG_COL_LEN
     , NUM_NULLS
     , LOW_VALUE
     , HIGH_VALUE
  FROM
       DICTIONARY_SCHEMA.USER_TAB_COLUMNS
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'ORDERS'
;


--# result: n rows
SELECT 
       TABLE_NAME
     , INDEX_NAME
     , DISTINCT_KEYS
  FROM
       DICTIONARY_SCHEMA.USER_INDEXES
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'ORDERS'
;


--##############################################
--# LINEITEM
--##############################################

--# result: 1 row
SELECT 
       TABLE_NAME
     , NUM_ROWS
  FROM
       DICTIONARY_SCHEMA.USER_TABLES
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'LINEITEM'
;

--# result: n rows
SELECT DISTINCT
       TABLE_NAME
     , GROUP_NAME
     , NUM_ROWS
  FROM
       DICTIONARY_SCHEMA.USER_TAB_PLACE
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'LINEITEM'
 ORDER BY 1, 2
;

--# result: n rows
SELECT 
       TABLE_NAME
     , COLUMN_NAME
     , NUM_DISTINCT
     , AVG_COL_LEN
     , NUM_NULLS
     , LOW_VALUE
     , HIGH_VALUE
  FROM
       DICTIONARY_SCHEMA.USER_TAB_COLUMNS
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'LINEITEM'
;


--# result: n rows
SELECT 
       TABLE_NAME
     , INDEX_NAME
     , DISTINCT_KEYS
  FROM
       DICTIONARY_SCHEMA.USER_INDEXES
 WHERE
       TABLE_SCHEMA = 'PUBLIC'
   AND TABLE_NAME   = 'LINEITEM'
;

