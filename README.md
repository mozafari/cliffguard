# CliffGuard

CliffGuard is a historical research suite for workload analysis and robust physical database design. It is preserved for research reproducibility and is not maintained or qualified for current DBMS or Java toolchains.

The supported historical targets are HP Vertica 6.x/7.x and Microsoft SQL Server 2008+. The checked-in build targets Java 8 and Maven 3.2+; its third-party driver and native-library assumptions have not been qualified against current releases.

## Quick start

Build the historical release with:

```bash
mvn clean package
```

For an offline smoke example that does not contact a database, run WorkloadMiner from the repository root:

```bash
java -cp target/CliffGuard.jar edu.umich.robustopt.experiments.WorkloadMiner \
  src/test/resources/ParserTest/basic0_schema.txt \
  src/test/resources/MinerTest/sample_query.txt
```

The command reports query and schema statistics. It validates the parser/miner path only; the CliffGuard Designer requires a separately configured, supported historical database deployment. Run from the repository root, or set `-Dcliffguard.base.path=/path/to/project` for external datasets. Copy `src/main/resources/databases.conf` before using the designer: it is an example template, not usable credentials. Historical cache-clearing support also requires the deployment-specific `cliffguard.ssh.user` and `cliffguard.restart-db.path` Java properties.

## Citing CliffGuard

```bibtex
@inproceedings{mozafari2015cliffguard,
  title     = {CliffGuard: A Principled Framework for Finding Robust Database Designs},
  author    = {Mozafari, Barzan and Goh, Eugene Zhen Ye and Yoon, Dong Young},
  booktitle = {ACM SIGMOD International Conference on Management of Data},
  year      = {2015},
  doi       = {10.1145/2723372.2749454}
}
```

Author: Barzan Mozafari (mozafari@umich.edu)

Table of Contents:
+ [About CliffGuard](#about-cliffguard)
+ [List of Tools](#list-of-tools)
+ User Guide
    + [Usage for CliffGuard Designer](#cliffguard-designer)
    + [Usage for WorkloadMiner](#workloadminer)
    + [Usage for VTester](#vtester)
+ [License](#license)

## About CliffGuard

CliffGuard is an open-source software suite for analyzing database workloads, deriving performance insights, and most importantly delivering robust physical designs that remain effective in spite of workload changes.

CliffGuard is developed by Barzan Mozafari et al. at the University of Michigan, Ann Arbor. For any inquiry, please contact Zhengxu Chen (zhxchen@umich.edu), who is actively contributing to the framework.

For more documentations and information visit [http://cliffguard.org](http://cliffguard.org)

CliffGuard currently supports the following database management systems:

- HP Vertica (Version 6.x.x and Version 7.x.x)
- Microsoft SQL Server 2008+

## List of Tools

CliffGuard comes with a number of tools. The current documentation describes the following ones:

+ [CliffGuard Designer](#cliffguard-designer): The core engine that can find a robust design for any database management system. The produced physical design is guaranteed to be robust (i.e., stay optimal) against future changes of workload as long as the future workload remains within a user-specified range of the original workload.
	
+ [WorkloadMiner](#workloadminer): An auxiliary tool that analyzes a set of past SQL queries, computes various statistics about their nature, and mines their underlying distribution over time.

+ [Scrubber](#scrubber): A performance-preserving workload scrubber, scrubbing a database schema, database histogram, set of SQL queries.

## User Guide

### CliffGuard Designer 

Usage:

```bash
$ java -jar CliffGuard.jar db_login_file deployer_db_alias designer_db_alias cliffGuard_config_file cliffGuard_setting_id query_file cache_directory local_path_to_stats_file distance_value(>=0 & <=1) output_suggested_design_filename shouldDeploy(t/f) [output_deployment_script_filename]
```
Parameter description:

+ db_vendor: either 'vertica' or 'microsoft' (without quotations)

+ db_login_file: an xml file with the login information (see databases.conf as an example)

+ deployer_db_alias, designer_db_alias: the short names of the entries in db_login_file for the two servers in the cluster intended for use by CliffGuard. CliffGuard uses the designer_db_alias server to invoke Vertica's internal designer with different parameters. CliffGuard uses deployer_db_alias to test different designers by deploying them on deployer_db_alias before deploying them on the entire cluster. We recommend that you have an empty instance of your database on designer_db_alias (identical schema as your actual database but with no user data) but have a non-empty database on deployer_db_alias. However, instead of copying your entire data to deployer_db_alias, you can alternatively copy a smaller sample of your entire data onto deployer_db_alias.

Note: deployer_db_alias and designer_db_aliass can be the same. These values are simply the unique ID of the login information entries in the  db_login_file.

+ cliffGuard_config_file: an XML file with parameter values for CliffGuard's deisgn algorithm (see cliffguard.conf as an example) 

+ cliffGuard_setting_id: the bean ID of the entry with class="edu.umich.robustopt.algorithms.NonConvexDesigner" in the cliffGuard_config_file that  you wish to use for loading the algorithm parameters from.

+ query_file: a CSV file (using | as separators) with timestamp followed by a single query in each line

	For example:
	 
```
	2011-12-14 19:38:51|select avg(salary) from employee
	2011-12-14 19:38:51|commit
```

+ cache_directory: a directory where CliffGuard algorithm can load a previously generated cache file. If such a cache file does not exist in this directory, then CliffGuard will create a new cache file there.

+ local_path_to_stats_file: the file path to the statistics file on the server hosting designer_db_alias. Note that you need to manually export the statistics from your actual database and copy the statistics file to server hosting designer_db_alias. This file does not have to be on the same server that you are running the CliffGuard tool on.

+ distance_value: a number between 0 and 1. A typically reasonable value is 0.01. To achieve the best results, you can run the WorkloadMiner tool on your past workload to gain insight into how each window of your queries have changed in the past. For example, if you would like to run your designer once a month, then you need to use your WorkloadMiner on your past query logs first to measure how much your workload has varied each month compared to the previous month. Then, you can simply take the average of the change between consecutive months, or their 90% quantile, maximum or even 3 times the maximum value or even higher. The higher this value, the greater the degree of robustness. The downside of higher values of distance_value is that you may lose too much optimality unnecessarily. Moreover, note that the current implementation may not be able to find a design if your requested value of distance_value is too high (depending on how large your provided query file is). 

+ output_suggested_design_filename: the path to the file that you would like CliffGuard to write the design statements to. This file is generated for informational purposes and should NOT be used as a script. Note that if this file already exists, the previous content of this file will be erased once CliffGuard is invoked.

+ shouldDeploy(t/f): a Boolean value specifying whether you would like Cliffguard to deploy the final design. If you set this option to "f", you could deploy the design manually using deployment script generated by cliffguard.

Optional parameter:

+ *output_deployment_script_filename*: the path to the file that you would like CliffGuard to write the deployment statements to. You can run this file on the database to deploy the suggested design. Note that if this file already exists,the previous content of this file will be erased once CliffGuard is invoked. You only need to specify this option when you set shouldDeploy to false.


### WorkloadMiner

**WorkloadMiner Usage**:

```bash
java -cp CliffGuard.jar edu.umich.robustopt.experiments.WorkloadMiner schema_file query_file <options>
```
Example:
(assume the working directory is the top level `cliffguard/` directory.)
```bash
java -cp ./target/CliffGuard.jar edu.umich.robustopt.experiments.WorkloadMiner src/test/resources/ParserTest/basic0_schema.txt src/test/resources/MinerTest/sample_query.txt
```

Argument description:
	
+ `schema_file`: a file describing the schema of the parsed query file in form of data definition language(DDL).

	For example:
	
```SQL
CREATE TABLE Station (
ID INT PRIMARY KEY,
CITY CHAR(20),
STATE CHAR(2),
LAT_N REAL,
LONG_W REAL
);

CREATE TABLE STATS
(ID INTEGER REFERENCES STATION(ID),
MONTH INTEGER CHECK (MONTH BETWEEN 1 AND 12),
TEMP_F REAL CHECK (TEMP_F BETWEEN -80 AND 150),
RAIN_I REAL CHECK (RAIN_I BETWEEN 0 AND 100),
PRIMARY KEY (ID, MONTH)
);
```
+ `query_file`: a sql query file

	For example: 
```SQL
	select avg(salary) from employee;
	select id from employee where id>2 order by id desc;
```

+ `options`:

|Option                                     |Description                                                                                                                                                                        |
|------------------------------------------ |-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|`None`                                     |Equivalent to "-g -p -c -j -a" when no argument is specified.                                                                                                                      |
|`-g <clauses>` or `--general<clauses>`     |General statistics on usage frequencies of different tables and columns, will print popular TABLES/COLUMNS in terms of number of queries in which they appear within the specified <clauses>. <clauses> can be any subset of parameters: `[s | w | f | g | o]`. Each of s / w / f / g / o parameter means only columns/tables appearing in SELECT/WHERE/FROM/GROUP BY/ORDER BY clause are considered respectively. If multiple parameters (e.g. "swg") are specified, the union of statistics for each corresponding parameter will be taken. If <clauses> is omitted, all parameters (e.g. "swfgo") will be taken.                           |
|`--multiple-occurrence`                    |Used with --general/-g. When specified, the multiple occurrence of the same tables/columns within the same query is counted multiple times instead of counted single time.                                                                                    |
|`-c <clauses>` or `--combination <clauses>`|Popular combinations of columns in terms of number of queries in which they co-appear. <clauses> has the same usage as the one in --general section. |
|`-j` or `--join`                           |Popular joined column groups. Number of queries that have at least join, number of queries that involve joining exactly two tables, and number of queries that involve joining three or more tables.  |
|`-a <clauses>` or `--aggregate <clauses>`  |Popular columns in terms of the number of times they have appeared as a parameter to aggregate functions. Number of queries that have min/max, sum/count/avg aggregates at least once, and number of queries that have min/max, sum/count/avg aggregates in SELECT/WHERE/GROUP BY clause. <clauses> has the same usage as the one in --general section.                |
|`--aggregate-type <types>`                 |Used with --aggregate/-a. <types> can be any subset of parameters: `[m | t]`. Parameter `m` means only max/min aggregate functions are considered in statistics. Parameter `t` means only sum/count/avg aggregate functions are considered in statistics. If multiple parameters (e.g. "mt") are specified, the union of statistics for each corresponding parameter will be taken. <types> is a required parameter. Using -a without --aggregate-type is equivalent to "-a --aggregate-type mt". |
|`-d` or `--distance`                       |Display distance among queries. The distance is a number between 0 and 1.                                                                                                                                                     |
|`--all`                                    |Output all available statistics. Equivalent to: "-g -p -c -j -a -d".                                                                                                               |
|`--help`                                   |Display this message.                                                                                                                                                         |
|`--column-only`                            |Only output COLUMN related statistics. Should be used with -g or -p .                                                                                                                                       |
|`--table-only`                             |Only output TABLE related statistics. Should be used with -g or -p .                                                                                                                                     |
|`--show-schema`                            |Display the schema used in the query file.                                                                                                         |


### VTester

VTester is a data generation tool.

Given a schema SQL file (e.g, schema.sql) and a statistics .xml file,
creates data files that conform to the distributions specified in the
statistics file.

Example use:

```bash
$ cd VTester
$ ./loadstats.sh  ../dataset_example_orig/schema.sql  ../dataset_example_orig/stats.xml 
```

The above command leaves data files in .dat files in the data directory. For example:

```
du -s -h data/*
4.0K	data/PUBLIC.NATION.dat
133M	data/PUBLIC.PART.dat
27M	data/PUBLIC.PARTSUPP.dat
4.0K	data/PUBLIC.REGION.dat
161M	data/PUBLIC.SUPPLIER.dat
```

These files may be bulk loaded into Vertica using the copy
command. For example:

```bash
$ vsql -f ../dataset_example_orig/design # create tables
$ cat data/PUBLIC.PARTSUPP.dat | vsql -c "COPY PUBLIC.PARTSUPP FROM STDIN DELIMITER '|' DIRECT"
```

#### dataset_example_orig

Example metadata from a database which contains the TPCH
benchmark. This example data is used to develop and test the various
scrubbing scripts.

## License

Copyright [2016] [Barzan Mozafari]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
