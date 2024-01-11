# TPC-BiH dbgen v1.0

## 简介
TPC-BiH dbgen工具用于生成双时态TPC-H数据。TPC-H数据是一个虚拟的批发分销业务的数据集，用于评估和比较大规模联机分析处理（OLAP）系统的性能。TPC-BiH dbgen工具为该数据集增加有效时间和事务时间两个时态维度的数据，并且增加随着时间推移所产生的历史数据（即更新数据集的SQL语句）。这些数据可用于评估时态数据库的性能。这个工具借助PostgreSQL数据库和Python脚本，将TPC-H基准测试工具生成的数据加工成初始状态的时态TPC-H数据和历史数据。这个工具对加工过程使用的SQL进行了优化，还建立索引，使得在100GB的TPC-H数据上的每个查询和更新都在毫秒级。

## 依赖的软件
* PostgreSQL - 12.17以上版本
* TPC-H基准测试软件 - 2以上版本
* Python - 3.8以上版本

## 使用方法
1. 首先下载安装12.17版本以上的PostgreSQL数据库、TPC-H基准测试软件以及3.8版本以上的Python。还要下载python的psycopg2模块。

2. 编译TPC-H基准测试软件获得`dbgen`工具。使用`dbgen`工具生成所有TPC-H的数据表。具体细节可参考章节[“编译生成合适的dbgen工具和TPC-H数据表”](#编译生成合适的dbgen工具和tpc-h数据表)。

3. 确保PostgreSQL在本地正确安装，并且有一个可以连接数据库的用户。

4. 按实际情况填写config.py文件中的配置参数。具体填写规则参见第5和第6步。

5. 参数`dbname`需要填写连接的PostgreSQL的数据库名称。参数`user`需要填写连接PostgreSQL的用户名。参数`password`需要填写连接PostgreSQL的用户的密码。参数`host`需要填写PostgreSQL服务器的IP地址。参数`port`需要填写PostgreSQL服务器的端口号。以上所有参数填写为Python字符串形式。默认值参考[The psycopg2 module content](https://www.psycopg.org/docs/module.html#module-psycopg2)。

6. 参数`tpchTblPath`需要填写生成的所有TPC-H数据表所在的文件夹路径。参数`psqlPath`需要填写PostgreSQL终端`psql`的路径。参数`destPath`需要填写本工具生成的双时态TPC-H数据的存储位置。以上路径参数填写为Python字符串形式。参数`v1Only`需要填写是否仅生成初始状态的时态TPC-H数据还是同时生成初始数据和历史数据，`True`表示仅生成初始状态的时态TPC-H数据，`False`表示同时生成初始数据和历史数据。参数`updateTimes`需要填写随时间推移对于初始状态数据进行修改的事务的数量，即更新次数，应填写为Python整型形式。

7. 执行dbgen.py文件。如使用`python3 dbgen.py`执行该文件。程序随后会在参数`destPath`指定的位置生成初始状态的时态TPC-H数据和历史数据。bi-【表名】.tbl文件（其中表名是各TPC-H数据表的表名）为初始状态的时态TPC-H数据。history.sql文件为历史数据。

8. 将初始数据载入要进行基准测试的数据库，然后在这个数据库中执行历史数据的SQL，就生成了双时态TPC-H数据。

## 编译生成合适的dbgen工具和TPC-H数据表
要编译生成合适的dbgen工具和TPC-H数据表，首先需要修改TPC-H基准测试软件目录下的makefile.suite文件。其中最重要的是填写合适的C编译器（`CC`）、数据库（`DATABASE`）、操作系统（`MACHINE`）、工作负载（`WORKLOAD`）以及修改`CFLAGS`。由于本工具采用PostgreSQL处理TPC-H数据表，因此`DATABASE`填写`ORACLE`，并且在`CFLAGS`最后增加`-DEOL_HANDLING`用于将每行末尾的结束符`|`去掉。一种填写方案如下：
```makefile
CC = gcc
DATABASE = ORACLE
MACHINE = LINUX
WORKLOAD = TPCH
CFLAGS	= -g -DDBNAME=\"dss\" -D$(MACHINE) -D$(DATABASE) -D$(WORKLOAD) -DRNG_TEST -D_FILE_OFFSET_BITS=64 -DEOL_HANDLING
```
之后，使用`make`编译，得到`dbgen`工具。  

最后，使用`dbgen`工具分别生成nation.tbl, region.tbl, part.tbl, supplier.tbl, partsupp.tbl, customer.tbl, orders.tbl, lineitem.tbl共8个数据文件。例如使用`./dbgen -vf -s 1`生成规模系数为1的8个TPC-H数据表。

## 调试
本工具提供数据处理过程中SQL执行情况的调试。在dbgen.py文件中修改常量`SQL_DEBUG`为`True`，则执行dbgen.py文件时会将执行的SQL语句和执行时间输出到dbgen-sql.prof文件中。

## 测试
本工具提供对于自身全面的测试，包括对所有生成函数的测试和对生成数据的测试等。目前仅在Linux环境中进行了测试。测试代码在test-dbgen.py文件中。

## TODO
* [] TODO: Process data which is larger than 1TB by implement Citus
