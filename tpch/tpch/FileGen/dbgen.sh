#!/bin/sh

# 컴파일을 하기 전에 
# makefile.suite 을 이용하여 Makefile 을 생성한다.
# Makefile 안에 다음과 같이 변수의 값을 설정한다.
#
# DATABASE=DB2 
# MACHINE =LINUX 
# WORKLOAD =TPCH 

##########################
# 컴파일
##########################

# 컴파일을 수행하면 다음 실행파일이 생성된다.
#   dbgen : text 파일을 생성
#   qgen  : query 파일을 생성

make clean;
make;

##########################
# 데이터 파일 생성
##########################

# Scale Factor 값
# Default 로 1 값이며 이를 조정하여 Scalability 를 조정한다.

rm -rf *.tbl;
dbgen -s $1;
chmod 644 *.tbl;

