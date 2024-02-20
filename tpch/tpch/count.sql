--###########################################
--# Record 건수 확인
--###########################################

--# result: 5
SELECT COUNT(*) FROM region;

--# result: 25
SELECT COUNT(*) FROM nation;

--# result: 10000
SELECT COUNT(*) FROM supplier;

--# result: 150000
SELECT COUNT(*) FROM customer;

--# result: 200000
SELECT COUNT(*) FROM part;

--# result: 800000
SELECT COUNT(*) FROM partsupp;

--# result: 1500000
SELECT COUNT(*) FROM orders;

--# result: 6001215
SELECT COUNT(*) FROM lineitem;
