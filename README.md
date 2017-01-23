# CliffGuard

Author: Barzan Mozafari (mozafari@umich.edu)
[http://cliffguard.org](http://cliffguard.org)

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

#### dataset1

Real customer workload (diag_dump_19)

diag_dump 19 which has 414K queries spread across 439 days:

```
 case_id | diag_dump_id | node_count |   request_window    | request_count |  requests_per_second
---------+--------------+------------+---------------------+---------------+-----------------------
      14 |           19 |          8 | 439 03:15:12.261809 |        414170 |  0.010916069458591067
```

Broken down across months, we can see that in the first few months
vertica was running not much happened (4 queries got run in feb 2011),
but recently the query volume has been growing (e.g. 100k queries in
march 2012). I suspect this should show a changing workload.

```
 case_id | diag_dump_id | month_year |   request_window   | request_count | requests_per_second  | min_request_time        |       max_request_time
---------+--------------+------------+--------------------+---------------+----------------------+-------------------------------+-------------------------------
      14 |           19 |     201102 | 16 22:49:50.761726 |             4 | 0.000002731138352454 | 2011-02-02 02:10:31.51482-05  | 2011-02-19 01:00:22.276546-05
      14 |           19 |     201103 | 18 16:45:04.460291 |            18 | 0.000011142030518912 | 2011-03-11 11:38:23.203161-05 | 2011-03-30 05:23:27.663452-04
      14 |           19 |     201104 | 20 05:40:42.915028 |            19 | 0.000010866811742433 | 2011-04-08 02:07:30.396027-04 | 2011-04-28 07:48:13.311055-04
      14 |           19 |     201105 | 17 04:31:48.610414 |             8 | 0.000005386811404837 | 2011-05-11 02:03:54.511141-04 | 2011-05-28 06:35:43.121555-04
      14 |           19 |     201106 | 11 13:09:54.75848 |            20 | 0.000020044202307163 | 2011-06-11 01:37:11.70824-04  | 2011-06-22 14:47:06.46672-04
      14 |           19 |     201107 | 09:00:15.87109 |            11 | 0.000339339947689803 | 2011-07-21 23:57:35.961706-04 | 2011-07-22 08:57:51.832796-04
      14 |           19 |     201108 | 8 14:45:50.639127 |           228 | 0.000306307253618276 | 2011-08-16 01:24:46.91992-04  | 2011-08-24 16:10:37.559047-04
      14 |           19 |     201109 | 27 23:52:56.39362 |          2113 | 0.000873582198657741 | 2011-09-02 21:59:24.55228-04  | 2011-09-30 21:52:20.9459-04
      14 |           19 |     201110 | 28 13:44:50.479117 |          5872 | 0.002378588992695550 | 2011-10-03 08:08:17.303949-04 | 2011-10-31 21:53:07.783066-04
      14 |           19 |     201111 | 29 16:56:00.397289 |         43712 | 0.017031354510952480 | 2011-11-01 08:03:09.349658-04 | 2011-11-30 23:59:09.746947-05
      14 |           19 |     201112 | 30 07:06:05.921698 |         45982 | 0.017566701804465631 | 2011-12-01 08:03:49.387395-05 | 2011-12-31 15:09:55.309093-05
      14 |           19 |     201201 | 30 18:48:38.261207 |         51292 | 0.019284749346618129 | 2012-01-01 05:04:12.84072-05  | 2012-01-31 23:52:51.101927-05
      14 |           19 |     201202 | 28 23:55:34.51467 |         87234 | 0.034819302368286883 | 2012-02-01 00:02:49.196844-05 | 2012-02-29 23:58:23.711514-05
      14 |           19 |     201203 | 30 15:47:39.486123 | 100712 | 0.038020891831981242 | 2012-03-01 00:01:02.921419-05 | 2012-03-31 16:48:42.407542-04
      14 |           19 |     201204 | 15 05:19:05.87348 |         76945 | 0.058506817799911636 | 2012-04-01 01:06:37.903149-04 | 2012-04-16 06:25:43.776629-04
(15 rows)
```

#### dataset_example_orig

Example diag_dump from a database which contains the TPCH
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

