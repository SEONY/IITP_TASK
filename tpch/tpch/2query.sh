####################################
# 지난 Report 삭제
####################################

gsql sys gliese << EOF
DELETE FROM TPCH_SF1_REPORT;
COMMIT;
\q
EOF


####################################
# Query 수행
# - Query Power Random Code 0 에 의한 수행
####################################

gsql sys gliese -i SF1/Q14.sql > SF1/Q14.lst # Promotion Effect Query (Q14)
gsql sys gliese -i SF1/Q02.sql > SF1/Q02.lst # Minimum Cost Supplier Query (Q2)
gsql sys gliese -i SF1/Q09.sql > SF1/Q09.lst # Product Type Profit Measure Query (Q9)
gsql sys gliese -i SF1/Q20.sql > SF1/Q20.lst # Potential Part Promotion Query (Q20)
gsql sys gliese -i SF1/Q06.sql > SF1/Q06.lst # Forecasting Revenue Change Query (Q6)
gsql sys gliese -i SF1/Q17.sql > SF1/Q17.lst # Small-Quantity-Order Revenue Query (Q17)
gsql sys gliese -i SF1/Q18.sql > SF1/Q18.lst # Large Volume Customer Query (Q18)
gsql sys gliese -i SF1/Q08.sql > SF1/Q08.lst # National Market Share Query (Q8)
gsql sys gliese -i SF1/Q21.sql > SF1/Q21.lst # Suppliers Who Kept Orders Waiting Query (Q21)
gsql sys gliese -i SF1/Q13.sql > SF1/Q13.lst # Customer Distribution Query (Q13)
gsql sys gliese -i SF1/Q03.sql > SF1/Q03.lst # Shipping Priority Query (Q3)
gsql sys gliese -i SF1/Q22.sql > SF1/Q22.lst # Global Sales Opportunity Query (Q22)
gsql sys gliese -i SF1/Q16.sql > SF1/Q16.lst # Parts/Supplier Relationship Query (Q16)
gsql sys gliese -i SF1/Q04.sql > SF1/Q04.lst # Order Priority Checking Query (Q4)
gsql sys gliese -i SF1/Q11.sql > SF1/Q11.lst # Important Stock Identification Query (Q11)
gsql sys gliese -i SF1/Q15.sql > SF1/Q15.lst # Top Supplier Query (Q15)
gsql sys gliese -i SF1/Q01.sql > SF1/Q01.lst # Pricing Summary Report Query (Q1)
gsql sys gliese -i SF1/Q10.sql > SF1/Q10.lst # Returned Item Reporting Query (Q10)
gsql sys gliese -i SF1/Q19.sql > SF1/Q19.lst # Discounted Revenue Query (Q19)
gsql sys gliese -i SF1/Q05.sql > SF1/Q05.lst # Local Supplier Volume Query (Q5)
gsql sys gliese -i SF1/Q07.sql > SF1/Q07.lst # Volume Shipping Query (Q7)
gsql sys gliese -i SF1/Q12.sql > SF1/Q12.lst # Shipping Modes and Order Priority Query (Q12)


####################################
# Reporting
####################################

gsql sys gliese -i report.sql              # TPC-H SF-1 Result Report
