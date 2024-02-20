##########################################################################
# TPC-H SF1 Benchmark Test
##########################################################################

####################################
# Data 구축 
####################################

tpchUploadSF1.sql                # TPC-H SF-1 Data Upload
tpchAnalyzeSF1.sql               # TPC-H SF-1 Analyze Table

####################################
# Hint Query 수행 
####################################

SF1/HintQ01.sql  # Pricing Summary Report Query (Q1)
SF1/HintQ02.sql  # Minimum Cost Supplier Query (Q2)
SF1/HintQ03.sql  # Shipping Priority Query (Q3)
SF1/HintQ04.sql  # Order Priority Checking Query (Q4)
SF1/HintQ05.sql  # Local Supplier Volume Query (Q5)
SF1/HintQ06.sql  # Forecasting Revenue Change Query (Q6)
SF1/HintQ07.sql  # Volume Shipping Query (Q7)
SF1/HintQ08.sql  # National Market Share Query (Q8)
SF1/HintQ09.sql  # Product Type Profit Measure Query (Q9)
SF1/HintQ10.sql  # Returned Item Reporting Query (Q10)
SF1/HintQ11.sql  # Important Stock Identification Query (Q11)
SF1/HintQ12.sql  # Shipping Modes and Order Priority Query (Q12)
SF1/HintQ13.sql  # Customer Distribution Query (Q13)
SF1/HintQ14.sql  # Promotion Effect Query (Q14)
SF1/HintQ15.sql  # Top Supplier Query (Q15)
SF1/HintQ16.sql  # Parts/Supplier Relationship Query (Q16)
SF1/HintQ17.sql  # Small-Quantity-Order Revenue Query (Q17)
SF1/HintQ18.sql  # Large Volume Customer Query (Q18)
SF1/HintQ19.sql  # Discounted Revenue Query (Q19)
SF1/HintQ20.sql  # Potential Part Promotion Query (Q20)
SF1/HintQ21.sql  # Suppliers Who Kept Orders Waiting Query (Q21)
SF1/HintQ22.sql  # Global Sales Opportunity Query (Q22)

####################################
# Query 수행 
# - Query Power Random Code 0 에 의한 수행
####################################

SF1/Q14.sql  # Promotion Effect Query (Q14)
SF1/Q02.sql  # Minimum Cost Supplier Query (Q2)
SF1/Q09.sql  # Product Type Profit Measure Query (Q9)
SF1/Q20.sql  # Potential Part Promotion Query (Q20)
SF1/Q06.sql  # Forecasting Revenue Change Query (Q6)
SF1/Q17.sql  # Small-Quantity-Order Revenue Query (Q17)
SF1/Q18.sql  # Large Volume Customer Query (Q18)
SF1/Q08.sql  # National Market Share Query (Q8)
SF1/Q21.sql  # Suppliers Who Kept Orders Waiting Query (Q21)
SF1/Q13.sql  # Customer Distribution Query (Q13)
SF1/Q03.sql  # Shipping Priority Query (Q3)
SF1/Q22.sql  # Global Sales Opportunity Query (Q22)
SF1/Q16.sql  # Parts/Supplier Relationship Query (Q16)
SF1/Q04.sql  # Order Priority Checking Query (Q4)
SF1/Q11.sql  # Important Stock Identification Query (Q11)
SF1/Q15.sql  # Top Supplier Query (Q15)
SF1/Q01.sql  # Pricing Summary Report Query (Q1)
SF1/Q10.sql  # Returned Item Reporting Query (Q10)
SF1/Q19.sql  # Discounted Revenue Query (Q19)
SF1/Q05.sql  # Local Supplier Volume Query (Q5)
SF1/Q07.sql  # Volume Shipping Query (Q7)
SF1/Q12.sql  # Shipping Modes and Order Priority Query (Q12)


####################################
# Reporting
####################################

tpchReportSF1.sql              # TPC-H SF-1 Result Report

