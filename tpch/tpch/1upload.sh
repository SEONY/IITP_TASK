#!/bin/bash

sh filegenSF1.sh
gsql sys gliese -i table.sql

cd FileGen
rm *.log *.bad
gloader sys gliese -i -c region.ctl   -d region.tbl   -l region.log    --atomic -s -n
gloader sys gliese -i -c nation.ctl   -d nation.tbl   -l nation.log    --atomic -s -n 
gloader sys gliese -i -c supplier.ctl -d supplier.tbl -l supplier.log  --atomic -s -n
gloader sys gliese -i -c customer.ctl -d customer.tbl -l customer.log  --atomic -s -n
gloader sys gliese -i -c part.ctl     -d part.tbl     -l part.log      --atomic -s -n
gloader sys gliese -i -c partsupp.ctl -d partsupp.tbl -l partsupp.log  --atomic -s -n
gloader sys gliese -i -c orders.ctl   -d orders.tbl   -l orders.log    --atomic -s -n
gloader sys gliese -i -c lineitem.ctl -d lineitem.tbl -l lineitem.log  --atomic -s -n
cd ..

gsql sys gliese -i count.sql

gsql sys gliese -i index.sql

gsql sys gliese -i analyze.sql

