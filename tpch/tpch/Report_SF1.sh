#!/bin/sh

rm SF1.report.sql
rm SF1.report.result
rm SF1/SF1_*.log
rm SF1/HINT_SF1_*.log

echo "\SET LINESIZE 200" >> SF1.report.sql
echo "SELECT QRY, SEC, QPH FROM TPCH_SF1_REPORT ORDER BY 1;" >> SF1.report.sql

gsql test test --no-prompt --import SF1.report.sql >> SF1.report.result

grep "HINT_SF1_Q01" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q01.log
grep "HINT_SF1_Q02" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q02.log
grep "HINT_SF1_Q03" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q03.log
grep "HINT_SF1_Q04" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q04.log
grep "HINT_SF1_Q05" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q05.log
grep "HINT_SF1_Q06" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q06.log
grep "HINT_SF1_Q07" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q07.log
grep "HINT_SF1_Q08" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q08.log
grep "HINT_SF1_Q09" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q09.log
grep "HINT_SF1_Q10" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q10.log
grep "HINT_SF1_Q11" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q11.log
grep "HINT_SF1_Q12" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q12.log
grep "HINT_SF1_Q13" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q13.log
grep "HINT_SF1_Q14" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q14.log
grep "HINT_SF1_Q15" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q15.log
grep "HINT_SF1_Q16" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q16.log
grep "HINT_SF1_Q17" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q17.log
grep "HINT_SF1_Q18" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q18.log
grep "HINT_SF1_Q19" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q19.log
grep "HINT_SF1_Q20" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q20.log
grep "HINT_SF1_Q21" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q21.log
grep "HINT_SF1_Q22" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/HINT_SF1_Q22.log


grep "SF1_Q01" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q01.log
grep "SF1_Q02" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q02.log
grep "SF1_Q03" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q03.log
grep "SF1_Q04" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q04.log
grep "SF1_Q05" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q05.log
grep "SF1_Q06" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q06.log
grep "SF1_Q07" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q07.log
grep "SF1_Q08" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q08.log
grep "SF1_Q09" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q09.log
grep "SF1_Q10" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q10.log
grep "SF1_Q11" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q11.log
grep "SF1_Q12" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q12.log
grep "SF1_Q13" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q13.log
grep "SF1_Q14" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q14.log
grep "SF1_Q15" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q15.log
grep "SF1_Q16_DISPLAY"    SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q16.log
grep "SF1_Q16_NO_DISPLAY" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q16_NO_DISPLAY.log
grep "SF1_Q17" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q17.log
grep "SF1_Q18" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q18.log
grep "SF1_Q19" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q19.log
grep "SF1_Q20" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q20.log
grep "SF1_Q21" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q21.log
grep "SF1_Q22" SF1.report.result | awk '{ print("YVALUE="$3) }'  >> SF1/SF1_Q22.log

