# gop

gop는 Database 모니터링을 위한  Console 도구 입니다.   
gop를 사용하여 아래의 내용으로 database에 대한 상태를 확인 할 수 있습니다.   
   
* Query 추이
* System 자원 사용량
* 임계치 알람 ( 자료 수집  스크립트 발동 )

## env

Lang : JAVA SE 17 (61)  
Database Class : DriverManager
```
$ wget https://download.oracle.com/java/17/latest/jdk-17_linux-x64_bin.rpm
$ sudo rpm -ivh jdk-17_linux-x64_bin.rpm
$ sudo alternatives --config java


% ./gradlew -version

------------------------------------------------------------
Gradle 7.3
------------------------------------------------------------

Build time:   2021-11-09 20:40:36 UTC
Revision:     96754b8c44399658178a768ac764d727c2addb37

Kotlin:       1.5.31
Groovy:       3.0.9
Ant:          Apache Ant(TM) version 1.10.11 compiled on July 10 2021
JVM:          17.0.6 (Oracle Corporation 17.0.6+9-LTS-190)
OS:           Mac OS X 13.2.1 x86_64
```

## install
소스를 받아서 컴파일 합니다.

```
$ git clone https://github.com/columbus-sunjesoft/gop.git
'gop'에 복제합니다...
remote: Enumerating objects: 406, done.
remote: Counting objects: 100% (68/68), done.
remote: Compressing objects: 100% (68/68), done.
remote: Total 406 (delta 33), reused 35 (delta 0), pack-reused 338
오브젝트를 받는 중: 100% (406/406), 805.50 KiB | 15.79 MiB/s, 완료.
델타를 알아내는 중: 100% (232/232), 완료.

$ cd gop

$ ls
README.md     create_stacker.sh    gop.tar    gradle    gradlew    gradlew.bat  settings.gradle    source

$ tar -xf gop.tar

$ ls
README.md     create_stacker.sh    gop    gop.tar    gradle    gradlew    gradlew.bat  settings.gradle    source

$ cd  gop

$ ls
bin  data  lib
```

## start 
  
```
% ./bin/gop -config data/config.json -demon
Current dir : /home/test/gop/gop
Source jdbc url : jdbc:goldilocks://localhost:22581/
Write file : true
Write file path : data/
Write stacker : false
Write stacker : http://192.168.0.120:5108/dbs/
Write stacker db name: gop

                     time          exec       optdata       session     g-session          peer          lock       long_Tx
"2023-03-02 16:26:27.537"         78690           811             1             0             0             0             0
```

### build
```
$ ./gradlew build

BUILD SUCCESSFUL in 4s
7 actionable tasks: 7 executed
$ cd gop/build/distributions 
$ ls
gop.tar gop.zip
$ unzip gop.zip 
Archive:  gop.zip
   creating: gop/
   creating: gop/lib/
  inflating: gop/lib/gop.jar         
  inflating: gop/lib/goldilocks8.jar  
  inflating: gop/lib/guava-30.1.1-jre.jar  
  inflating: gop/lib/httpclient-4.5.14.jar  
  inflating: gop/lib/gson-2.10.1.jar  
  inflating: gop/lib/failureaccess-1.0.1.jar  
  inflating: gop/lib/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar  
  inflating: gop/lib/jsr305-3.0.2.jar  
  inflating: gop/lib/checker-qual-3.8.0.jar  
  inflating: gop/lib/error_prone_annotations-2.5.1.jar  
  inflating: gop/lib/j2objc-annotations-1.3.jar  
  inflating: gop/lib/httpcore-4.4.16.jar  
  inflating: gop/lib/commons-logging-1.2.jar  
  inflating: gop/lib/commons-codec-1.11.jar  
   creating: gop/bin/
  inflating: gop/bin/gop             
  inflating: gop/bin/gop.bat         
```


### Help
 
~~~
$ java -jar gop.jar -help
 ---
 -config <config file path> [ -demon | -client -log <log file path> <option> ]

 client option:
   -log <log file path> [ -time 'yyyy-mm-dd hh24:mi:ss.fff' 'yyyy-mm-dd hh24:mi:ss.fff' | -name <column name> | -tag <tag name> ]
			[ -head | -tail <print count> ]

 ---
 sample use
 java -Xmx100M -jar gop.jar -config resource/config.json -demon
 java -jar gop.jar -config resource/config.json -client -log resource/log_20221201.json -time '2022-12-01 03:14:40.000' '2022-12-01 03:15:00.000'
 java -jar gop.jar -config resource/config.json -client -log resource/log_20221201.json -name execute -tail 10
 java -jar gop.jar -config resource/config.json -client -log resource/log_20221201.json -tag tag1 -head 10
 java -jar gop.jar -config resource/config.json -client -log resource/log_20221201.json
~~~


## Config

#### config 명세  

~~~

setting : 설정
url(String) : DB 접속 정보 (host:port)  
	dbname(String) :  gop   
	driverClass(String) : "sunje.goldilocks.jdbc.GoldilocksDriver"
	jdbcProperties : DB id, password
	timeInterval : monitoring time interval  
	consolePrint : print monitoring log  
	pagesize : print column header between row  
	retention : delete log
	printCSV : print csv
	failLog : write log file
	
measure  
	name : String - monitoring column name  
	tag : String - monitoring tag name  
	diff : boolean - diff before values  
	alertValue : int  
	alertPolicy : int  
	sql : String - monitoring query  
	sqlIsOs : boolean - sql command is os command  
	alertScript : String   
	alertScriptIsOs : boolean  

~~~

#### alert policy :
- 0 : not use ( default )
- 1 : is greater then alertValue ( query result > alertValue )
- 2 : is less then alertValue ( query result < alertValue )
- 3 : equal to alertValue ( query result = alertValue )



#### config sample

```
{
	"setting": {
		"jdbcSource" : {
			"url":"jdbc:goldilocks://localhost:22581/",
			"dbName": "gop",
			"driverClass":"sunje.goldilocks.jdbc.GoldilocksDriver",
			"jdbcProperties":[
				{ "name":"user", "value":"test" },
				{ "name":"password", "value":"test" },
				{ "name":"statement_pool_on", "value":"true" },
				{ "name":"statement_pool_size", "value":"20" },
				{ "name":"login_timeout", "value":"3" }
			]
		},
		"timeInterval": 1000,
		"consolePrint": true,
		"pageSize": "10",
		"retention": "2",
		"printCSV": false,
		"fileLog" : {
			"enable":true,
			"logPath": "data/"
		},
		"stacker" : {
			"enable":true,
			"baseUrl": "http://192.168.0.120:5108/dbs/",
			"dbName":"gop"
		}
	},
	"measure": [
		{
			"name": "exec",
			"diff": true,
			"member" : "g1n1",
			"sql": "select stat_value from v$system_sql_stat where stat_name = 'CALL_EXECUTE'",
			"tag": "sql1"
		},
		{
			"name": "optdata",
			"diff": true,
			"sql": "select stat_value from v$system_sql_stat where stat_name = 'CALL_OPTDATA'",
			"tag": "sql1"
		},
		{
			"name": "session",
			"alertValue": 80,
			"alertPolicy": 1,
			"sql": "select count(*) from x$session@local where top_layer = 13",
			"tag": "sql1"
		},
		{
			"name": "g-session",
			"alertValue": 80,
			"alertPolicy": 1,
			"sql": "select count(*) from x$session@local where top_layer = 13 and global_connection is true",
			"tag": "sql1"
		},
		{
			"name": "peer",
			"alertValue": 80,
			"alertPolicy": 1,
			"sql": "select count(*) from x$session@local where program = 'cluster peer'",
			"tag": "sql1"
		},
		{
			"name": "lock",
			"alertValue": 0,
			"alertPolicy": 1,
			"alertSql": "select * from tech_lockwait",
			"sql": "select count(*) from v$lock_wait",
			"tag": "sql1",
			"alertScript": "echo \"select * from tech_lockwait;\"|gsqlnet sys gliese --no-prompt",
			"alertScriptIsOs": true
		},
		{
			"name": "long_Tx",
			"tag": "sql1",
			"alertValue": 0,
			"alertPolicy": 3,
			"sql": "select count(*) from x$transaction@local where datediff(SECOND,BEGIN_TIME,systimestamp) > 10",
			"alertScript":"free -wh",
			"alertScriptIsOs": true
		},
		{
			"name": "MemAvaimb",
			"tag": "sql1",
			"diff": false,
			"alertValue": 80,
			"alertPolicy": 1,
			"sql": "cat /proc/meminfo |grep MemAvailable|awk {'print int($2/1024)'}",
			"sqlIsOs": true,
			"alertScript":"select * from x$cluster_member",
			"alertScriptIsOs": false
		}
	]
}

```

