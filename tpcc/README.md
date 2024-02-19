# TPCC
TPC-C test suit for GOLDILOCKS DBMS

Based on BenchmarkSQL
Modified for GOLDILOCKS CLUSTER DBMS

configuration file : tpcc/run/props.goldilocks

HOW-TO-RUN
{{{
1. build database
$tpcc/run> ./runDatabaseBuild.sh props.goldilocks

2. test
$tpcc/run> ./runBenchmark.sh props.goldilocks
}}}
