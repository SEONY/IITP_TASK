--##########################################################################
--# TPC-H Scale Factor 1 Report
--##########################################################################

--##########################################
--# REPORT
--##########################################

SELECT VERSION() FROM DUAL;

--# refine result 
--# 오동작 query refinement
UPDATE TPCH_SF1_REPORT SET QPH = 0 WHERE QPH > 1000000;
COMMIT;

\set pagesize 100

--# elpased information
SELECT QRY, SEC, QPH FROM TPCH_SF1_REPORT ORDER BY 1;

--+EXEC sh Report_SF1.sh

