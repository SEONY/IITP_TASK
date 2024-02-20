--###################################################################
--# INDEX 생성
--###################################################################


--##########################################
--# region TABLE
--##########################################

--# primary key
ALTER TABLE region ADD CONSTRAINT region_pk PRIMARY KEY( r_regionkey );
COMMIT;

--##########################################
--# nation TABLE
--##########################################

--# primary key
ALTER TABLE nation ADD CONSTRAINT nation_pk PRIMARY KEY( n_nationkey );
COMMIT;

--# foreign key
CREATE INDEX nation_regionkey_fk ON nation( n_regionkey );
COMMIT;

--##########################################
--# supplier TABLE
--##########################################

--# primary key
ALTER TABLE supplier 
   ADD CONSTRAINT supplier_pk PRIMARY KEY( s_suppkey );
COMMIT;

--# foreign key
CREATE INDEX supplier_nationkey_fk ON supplier( s_nationkey );
COMMIT;

--##########################################
--# customer TABLE
--##########################################

--# primary key
ALTER TABLE customer 
   ADD CONSTRAINT customer_pk PRIMARY KEY( c_custkey );
COMMIT;

--# foreign key
CREATE INDEX customer_nationkey_fk ON customer( c_nationkey );
COMMIT;

--##########################################
--# part TABLE
--##########################################

--# primary key
ALTER TABLE part 
   ADD CONSTRAINT part_pk PRIMARY KEY( p_partkey );
COMMIT;


--##########################################
--# partsupp TABLE
--##########################################

--# primary key
ALTER TABLE partsupp 
   ADD CONSTRAINT partsupp_pk PRIMARY KEY( ps_partkey, ps_suppkey );
COMMIT;

--# foreign key
CREATE INDEX partsupp_partkey_fk ON partsupp( ps_partkey );
COMMIT;

--# foreign key
CREATE INDEX partsupp_suppkey_fk ON partsupp( ps_suppkey );
COMMIT;


--##########################################
--# orders TABLE
--##########################################

--# primary key
ALTER TABLE orders 
   ADD CONSTRAINT orders_pk PRIMARY KEY( o_orderkey );
COMMIT;

--# foreign key
CREATE INDEX orders_custkey_fk ON orders( o_custkey );
COMMIT;


--##########################################
--# lineitem TABLE
--##########################################

--# primary key
ALTER TABLE lineitem 
   ADD CONSTRAINT lineitem_pk PRIMARY KEY( l_orderkey, l_linenumber );
COMMIT;

--# foreign key
CREATE INDEX lineitem_partkey_suppkey_fk 
    ON lineitem( l_partkey, l_suppkey );
COMMIT;

--# foreign key
CREATE INDEX lineitem_orderkey_fk ON lineitem( l_orderkey );
COMMIT;

--##########################################
--# Report Table
--##########################################

DROP TABLE IF EXISTS TPCH_SF1_REPORT;
COMMIT;

CREATE TABLE TPCH_SF1_REPORT
(
    QRY   VARCHAR(32),
    SEC   NUMBER(10,2),
    QPH   NUMBER
);
COMMIT;

ALTER SYSTEM LOOPBACK AGER;
ALTER SYSTEM LOOPBACK AGER;

