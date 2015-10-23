Author: Barzan Mozafari (mozafari AT umich.edu)
http://cliffguard.org

Table of Contents:
     About CliffGuard
     List of Tools
     Usage Guide: WorkloadMiner
     Usage Guide: CliffGuard Designer
     Usage Guide: Scrubber
     Usage Guide: VTester
     Miscellaneous
     License

********************************************
* About CliffGuard
********************************************

CliffGuard is an open-source software suite for analyzing database workloads, deriving performance insights, and most importantly delivering robust physical designs that remain effective in spite of workload changes.

CliffGuard is developed by Barzan Mozafari et al. at the University of Michigan, Ann Arbor. For all inquiries, please contact mozafari <AT> umich.edu

For more documentations and information visit http://cliffguard.org

CliffGuard currently supports the following database management systems:
- HP Vertica (Version 6.x.x and Version 7.x.x)
- Microsoft SQL Server (Developer Edition and Enterprise Edition)

********************************************
* List of Tools
********************************************

CliffGuard comes with a number of tools. The current documentation describes the following ones:

* CliffGuard Designer: The core engine that can find a robust design for any database management system. The produced physical design is guaranteed to be robust (i.e., stay optimal) against future changes of workload as long as the future workload remains within a user-specified range of the original workload.
	
* WorkloadMiner: An auxiliary tool that analyzes as set of past SQL queries, computes various statistics about their nature, and mines their underlying distribution over time.

* Scrubber: A performance-preserving workload scrubber, scrubbing a database schema, database histogram, set of SQL queries, 

********************************************
* Usage Guide: CliffGuard Designer 
********************************************

Usage:
java -cp CliffGuard.jar edu.umich.robustopt.experiments.CliffGuard db_vendor db_login_file deployer_db_alias designer_db_alias cliffGuard_config_file cliffGuard_setting_id query_file cache_directory local_path_to_stats_file distance_value(>=0 & <=1) output_suggested_design_filename shouldDeploy(t/f) [output_deployment_script_filename]

db_vendor: either 'vertica' or 'microsoft' (without quotations)

db_login_file: an xml file with the login information (see databases.conf as an example)

deployer_db_alias, designer_db_alias: the short names of the entries in db_login_file for the two servers in the cluster intended for use by CliffGuard. CliffGuard uses the designer_db_alias server to invoke Vertica's internal designer with different parameters. CliffGuard uses deployer_db_alias to test different designers by deploying them on deployer_db_alias before deploying them on the entire cluster. We recommend that you have an empty instance of your database on designer_db_alias (identical schema as your actual database but with no user data) but have a non-empty database on deployer_db_alias. However, instead of copying your entire data to deployer_db_alias, you can alternatively copy a smaller sample of your entire data onto deployer_db_alias.
Note: deployer_db_alias and designer_db_aliass can be the same. These values are simply the unique ID of the login information entries in the  db_login_file.

cliffGuard_config_file: an XML file with parameter values for CliffGuard's deisgn algorithm (see cliffguard.conf as an example) 

cliffGuard_setting_id: the bean ID of the entry with class="edu.umich.robustopt.algorithms.NonConvexDesigner" in the cliffGuard_config_file that  you wish to use for loading the algorithm parameters from.

query_file: a CSV file (using | as separators) with timestamp followed by a single query in each line
	For example: 
	2011-12-14 19:38:51|select avg(salary) from employee
	2011-12-14 19:38:51|commit

cache_directory: a directory where CliffGuard algorithm can load a previously generated cache file. If such a cache file does not exist in this directory, then CliffGuard will create a new cache file there.

local_path_to_stats_file: the file path to the statistics file on the server hosting designer_db_alias. Note that you need to manually export the statistics from your actual database and copy the statistics file to server hosting designer_db_alias. This file does not have to be on the same server that you are running the CliffGuard tool on.

distance_value: a number between 0 and 1. A typically reasonable value is 0.01. To achieve the best results, you can run the WorkloadMiner tool on your past workload to gain insight into how each window of your queries have changed in the past. For example, if you would like to run your designer once a month, then you need to use your WorkloadMiner on your past query logs first to measure how much your workload has varied each month compared to the previous month. Then, you can simply take the average of the change between consecutive months, or their 90% quantile, maximum or even 3 times the maximum value or even higher. The higher this value, the greater the degree of robustness. The downside of higher values of distance_value is that you may lose too much optimality unnecessarily. Moreover, note that the current implementation may not be able to find a design if your requested value of distance_value is too high (depending on how large your provided query file is). 

output_suggested_design_filename: the path to the file that you would like CliffGuard to write the design statements to. This file is generated for informational purposes and should NOT be used as a script. Note that if this file already exists, the previous content of this file will be erased once CliffGuard is invoked.

shouldDeploy(t/f): a Boolean value specifying whether you would like Cliffguard to deploy the final design. If you set this option to "f", you could deploy the design manually using deployment script generated by cliffguard.

output_deployment_script_filename (Optional): the path to the file that you would like CliffGuard to write the deployment statements to. You can run this file on the database to deploy the suggested design. Note that if this file already exists,the previous content of this file will be erased once CliffGuard is invoked. You only need to specify this option when you set shouldDeploy to false.


********************************************
* Usage Guide: WorkloadMiner
********************************************

Usage:
java -cp CliffGuard.jar edu.umich.robustopt.experiments.WorkloadMiner db_vendor db_name db_login_file query_file output_dir output_dir [window_size_in_days number_of_initial_windows_to_skip number_of_windows_to_read]

db_vendor: either 'vertica' or 'microsoft' (without quotations)

db_alias: the short name of the database (e.g., tpch, employmentInfo). This is the ID associated to the target database in the 
db_login_file. In other words, the db_alias is used to find the appropriate login information from db_login_file

db_login_file: an xml file with the login information (see databases.conf as an example)

query_file: a CSV file (using | as separators) with timestamp followed by a single query in each line
	For example: 
	2011-12-14 19:38:51|select avg(salary) from employee
	2011-12-14 19:38:51|commit

output_dir: an empty directory to store the output of the analysis

window_size_in_days: number of days in each window, as a unit of anlysis (e.g., 7 for analyzing weekly patterns and 30 for analyzing monthly patterns). Default: 7

number_of_initial_windows_to_skip: to account for early stages of the database lifetime when few query had run. Default:0 

number_of_windows_to_read: to control the total number windows to analyze (choose -1 to read all windows of queries). Default: -1




********************************************
* Usage Guide: VTester
********************************************

VTester is a data generation tool.

Given a schema SQL file (e.g, schema.sql) and a statistics .xml file,
creates data files that conform to the distributions specified in the
statistics file.

Example use:
 cd VTester
 ./loadstats.sh  ../dataset_example_orig/schema.sql  ../dataset_example_orig/stats.xml 

The above command leaves data files in .dat files in the data directory. For example:

du -s -h data/*
4.0K	data/PUBLIC.NATION.dat
133M	data/PUBLIC.PART.dat
27M	data/PUBLIC.PARTSUPP.dat
4.0K	data/PUBLIC.REGION.dat
161M	data/PUBLIC.SUPPLIER.dat

These files may be bulk loaded into Vertica using the copy
command. For example:

 vsql -f ../dataset_example_orig/design # create tables
 cat data/PUBLIC.PARTSUPP.dat | vsql -c "COPY PUBLIC.PARTSUPP FROM STDIN DELIMITER '|' DIRECT"

******************
dataset1
******************
Real customer workload (diag_dump_19)


diag_dump 19 which has 414K queries spread across 439 days:


 case_id | diag_dump_id | node_count |   request_window    | request_count |  requests_per_second
---------+--------------+------------+---------------------+---------------+-----------------------
      14 |           19 |          8 | 439 03:15:12.261809 |        414170 |  0.010916069458591067


Broken down across months, we can see that in the first few months
vertica was running not much happened (4 queries got run in feb 2011),
but recently the query volume has been growing (e.g. 100k queries in
march 2012). I suspect this should show a changing workload.


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


******************
dataset_example_orig
******************

Example diag_dump from a database which contains the TPCH
benchmark. This example data is used to develop and test the various
scrubbing scripts.


********************************************
* Usage Guide: Scrubber
********************************************

TODO: description of how the scrubbing scripts work and how they are invoked


 scrubber.py  takes as input the stats.xml file and the out_dc_requests_issued file, and produces stats.xml-scrubbed and out_dc_requests_issued-scrubbed, such that the column names + strings in stats.xml-scrubbed are all scrubbed, and the column names + string constants in out_dc_requests_issued-scrubbed are also all scrubbed.

You will also need beautiful soup installed (http://www.crummy.com/software/BeautifulSoup/)

You also need to have the lxml installed (it can serve as a tree builder for xml inputs) http://lxml.de/installation.html

Note:
If lxml is giving you too much trouble, you can just replace the following line in scrubber.py:
	stats_xml = BeautifulSoup(sfp, ['xml']) # requires lxml
with:
	stats_xml = BeautifulSoup(sfp)



********************************************
* Miscellaneous
********************************************

******************
Available example data
******************

The breakdown of query requests per diag_dump is below.  Note that
request_window is the number of days of workload history (difference
between first and last request in the data)


-- Statistics of queries across requests
select
       request_summary.case_id,
       request_summary.diag_dump_id,
       node_counts.node_count,
       request_summary.request_window,
       request_summary.request_count,
       request_summary.requests_per_second
       --request_summary.min_request_time,
       --request_summary.max_request_time
from
(select
       case_id,
       diag_dump_id,
       max(time) - min(time) as request_window,
       count(*)  as request_count,
       count(*) / extract(epoch from max(time) - min(time)) as requests_per_second,
       min(time) as min_request_time,
       max(time) as max_request_time
 from vu.dc_requests_issued
-- where diag_dump_id = 185
 group by 1,2
) as request_summary
join
(select
       case_id, diag_dump_id, count(*) as node_count
       from vu.nodes
       group by 1,2
) as node_counts
USING (case_id, diag_dump_id)
order by 4 desc;


 case_id | diag_dump_id | node_count |   request_window    | request_count |  requests_per_second
---------+--------------+------------+---------------------+---------------+-----------------------
     103 |          133 |          3 | 453 00:16:09.392542 |        113513 |  0.002900166293649956
     149 |          185 |         60 | 445 05:25:13.164529 |       3016065 |  0.078405510309440067
      14 |           19 |          8 | 439 03:15:12.261809 |        414170 |  0.010916069458591067
      12 |           15 |          8 | 435 03:52:35.877234 |        405230 |  0.010777979561526415
      12 |           21 |          8 | 433 22:58:28.48677  |        395049 |  0.010536351528659540
      12 |           16 |          8 | 433 01:53:46.834369 |        389166 |  0.010400494973255655
     114 |          148 |         60 | 431 19:10:16.040997 |       3085211 |  0.082696989697964381
      58 |           76 |         22 | 411 22:17:24.734776 |        576454 |  0.016196784416123280
      63 |           83 |          4 | 363 09:53:26.478443 |        279820 |  0.008911803707956599
      12 |           78 |          8 | 304 19:57:35.130542 |        333085 |  0.012646817938523638
      64 |           84 |          8 | 298 06:57:26.831817 |        336578 |  0.013059707296628841
      12 |           86 |          8 | 293 08:36:28.753632 |        356591 |  0.014068821291678499
      12 |           79 |          8 | 291 09:06:38.542027 |        353131 |  0.014026940022359299
     330 |          428 |          6 | 287 00:46:09.482226 |        535532 |  0.021594407128068049
      14 |           20 |          8 | 282 13:01:52.415329 |        321054 |  0.013151637809660520
     147 |          182 |          3 | 269 23:32:04.97213  |        202997 |  0.008702485292584163
     237 |          298 |          4 | 267 10:40:46.882267 |        316991 |  0.013718250452556758
     353 |          452 |          6 | 265 18:45:25.819055 |        372132 |  0.016205351170037096
     237 |          299 |          4 | 265 12:45:25.283375 |        303431 |  0.013226047781608070
     307 |          387 |          2 | 261 07:34:07.602851 |        123397 |  0.005465449818803001
     314 |          406 |         32 | 259 00:37:18.709682 |       2510099 |  0.112158940578694928
     235 |          296 |          4 | 247 07:44:35.427609 |        294120 |  0.013764072602272189
      67 |           88 |         16 | 238 00:58:38.925169 |        343901 |  0.016721237901449762
      92 |          120 |         39 | 219 02:25:21.631971 |        528277 |  0.027906393259996423
     281 |          350 |          4 | 209 00:11:38.005896 |         91827 |  0.005085030713859006
     282 |          351 |          3 | 201 03:10:49.644491 |         94264 |  0.005424376544187841
     323 |          417 |          4 | 196 08:23:28.587447 |        228568 |  0.013473225675783018
      96 |          126 |          5 | 183 19:22:55.299072 |        126251 |  0.007949826608405936
      97 |          127 |          3 | 178 20:04:11.564485 |        158701 |  0.010270944405299279
      48 |           63 |          3 | 177 21:51:17.264591 |        128093 |  0.008333161334796943
      48 |           65 |          3 | 176 20:14:17.047581 |        127005 |  0.008312249712436596
     196 |          248 |          4 | 173 16:16:11.512734 |        100950 |  0.006727411510586653
     196 |          247 |          4 | 173 15:44:43.428687 |        100635 |  0.006707263521361992
     110 |          141 |          3 | 168 02:52:02.1165   |         90595 |  0.006236953086670136
     321 |          414 |          4 | 161 23:07:09.606434 |        174008 |  0.012434801041181945
     284 |          353 |          6 | 160 20:55:24.662252 |        340528 |  0.024499607590634327
     278 |          348 |          4 | 159 02:06:41.141187 |         98009 |  0.007130415844284705
     266 |          333 |          6 | 159 02:01:02.817955 |        240137 |  0.017471036501456205
     233 |          293 |          8 | 156 15:00:20.93862 |         597308 |  0.044139034893257752
     233 |          291 |          8 | 152 21:19:28.716136 |        565685 |  0.042823881093785738
      89 |          116 |          3 | 136 14:24:05.087052 |         75359 |  0.006385141084951270
      88 |          114 |          3 | 135 22:29:00.11842 |          87398 |  0.007441332107171041
     121 |          153 |          8 | 134 17:22:21.366033 |        511087 |  0.043907284622109379
      90 |          118 |          3 | 132 11:46:37.38215 |          97952 |  0.008556854287560364
      30 |           42 |          8 | 131 02:03:47.414737 |        345682 |  0.030521566976219651
     118 |          150 |          6 | 129 09:55:23.490462 |        495605 |  0.044324359314240907
     330 |          425 |          6 | 128 16:37:02.441018 |        560112 |  0.050374212568701233
      30 |           40 |          8 | 127 03:26:53.453888 |        374052 |  0.034050499024906217
     279 |          349 |          4 | 124 19:46:49.101255 |         98589 |  0.009141469178951666
      30 |           44 |          8 | 124 05:38:17.529961 |        319627 |  0.029777347800073634
     339 |          437 |          5 | 123 20:41:27.354445 |        105295 |  0.009839102611819919
       6 |            6 |         24 | 123 09:44:25.994813 |        816672 |  0.076594600096949015
      83 |          110 |          5 | 123 08:01:41.963922 |        421502 |  0.039554989378579982
     330 |          427 |          6 | 122 14:35:36.68253  |        583295 |  0.055062443258500495
     301 |          380 |          6 | 118 12:29:19.6504   |        301062 |  0.029400127564245539
     330 |          426 |          6 | 118 08:22:54.300699 |        545977 |  0.053394329043062743
     169 |          211 |          4 | 114 22:06:54.18165  |         40906 |  0.004119762072974278
     262 |          328 |          3 | 114 18:22:57.194223 |        375781 |  0.037897281538247208
     115 |          149 |          3 | 113 01:48:05.332629 |        224620 |  0.022991528626803303
     107 |          138 |          8 | 112 02:22:34.730024 |        172738 |  0.017834968859170733
     107 |          142 |          8 | 112 02:06:30.992288 |        166541 |  0.017196848013739025
     201 |          254 |          4 | 107 06:15:52.0731   |        260361 |  0.028094432794426818
     289 |          361 |          8 | 104 02:29:18.4273   |        148125 |  0.016468290377703887
     257 |          323 |          7 | 103 14:48:33.80823  |        843356 |  0.094203261571594244
     335 |          433 |         16 | 99 06:09:39.012068  | 1115718 |  0.130101067020260238
     289 |          362 |          8 | 98 23:32:14.49973   |        141091 |  0.016498138521109405
      42 |           56 |          6 | 94 21:42:14.712642  |        298896 |  0.036451911003800518
     291 |          367 |          3 | 91 07:22:50.190957  |        172322 |  0.021843408686919611
     119 |          151 |          4 | 90 20:50:42.154379  |        154524 |  0.019681973037657509
     290 |          363 |          3 | 89 13:23:21.552813  |        163757 |  0.021163246289311696
     311 |          403 |         20 | 89 11:43:55.703915  | 1600829 |  0.207043845899289265
      10 |           13 |         15 | 87 19:43:36.033056 |        630028 |  0.083031533349689770
     104 |          135 |          4 | 87 16:33:01.220723 |         88130 |  0.011632202423888844
     291 |          365 |          3 | 87 10:26:46.955833 |        170252 |  0.022536778994748617
     291 |          366 |          3 | 87 10:02:10.189928 |        170027 |  0.022511395673527445
      16 |           23 |          6 | 87 08:42:28.777037 |        322547 |  0.042731934614385638
     233 |          294 |          3 | 86 18:44:59.549678 |          7069 |  0.000942797373206151
     233 |          292 |          3 | 86 18:27:16.624678 |          6816 |  0.000909183478477198
       9 |           11 |          6 | 85 15:47:47.657513 |        319916 |  0.043226823502949262
     288 |          357 |          8 | 84 03:40:51.006003 |        128206 |  0.017632874046538687
      69 |           90 |          3 | 83 14:12:42.163325 |         84191 |  0.011656989513419874
       9 |           10 |          6 | 83 10:27:25.54673 |        298842 |  0.041454903987443251
     288 |          358 |          8 | 82 02:33:06.712207 |        139850 |  0.019713879610086198
     288 |          359 |          8 | 81 02:45:02.287876 |        154600 |  0.022059550751321043
     288 |          360 |          8 | 78 07:24:33.24794 |        165279 |  0.024428332299946997
      18 |           25 |         10 | 78 03:55:02.993005 |        119673 |  0.017720662041071759
     236 |          297 |          4 | 72 18:51:21.474853 |        196252 |  0.031207177654770224
     137 |          172 |          3 | 71 23:18:01.449585 |        129144 |  0.020768439165554158
      78 |          104 |         16 | 71 03:15:39.568104  | 1236330 |  0.201155536137847728
     255 |          316 |          3 | 69 14:23:22.675098 |        224643 |  0.037357052593578229
      34 |           47 |         15 | 68 21:43:34.704337 |        337824 |  0.056744577150639073
      68 |           89 |         16 | 68 21:33:40.092106 |        205261 |  0.034481304125450627
     194 |          243 |          3 | 68 19:14:05.955526 |         80349 |  0.013516650769666261
     304 |          384 |          4 | 67 23:04:18.200801 |        247545 |  0.042157864092534038
     261 |          327 |          3 | 67 01:43:50.551683 |        382271 |  0.065965312277599706
     305 |          385 |          4 | 66 21:07:15.769598 |        234915 |  0.040653735607126564
      70 |           96 |          8 | 66 15:36:41.594554  | 1021142 |  0.177324647873836251
      75 |           99 |          1 | 66 14:55:21.422124 |         91237 |  0.015850430056830470
     275 |          346 |          3 | 66 09:13:26.008393 |        113491 |  0.019787098317758731
      37 |           49 |         15 | 65 00:36:33.246942 |        367427 |  0.065399494793097702
     270 |          337 |          3 | 64 18:15:07.215069 |         84465 |  0.015095685858414192
     218 |          274 |          3 | 60 17:42:24.903534 |         81263 |  0.015485318264093761
     263 |          329 |          3 | 59 17:46:13.391942 |        239152 |  0.046333158872322264
      84 |          111 |         15 | 59 01:20:42.370405 |        267228 |  0.052372566038171463
      84 |          117 |         15 | 59 01:20:42.370405 |        267228 |  0.052372566038171463
     256 |          322 |          3 | 59 00:40:58.322418 |        344526 |  0.067553345122660482
     285 |          354 |          6 | 55 00:46:15.077571 |        369633 |  0.077739323936397180
     123 |          157 |          5 | 54 18:25:13.141617 |        489641 |  0.103476328779923204
      70 |           91 |          8 | 54 18:11:07.15402 |        953425 |  0.201524300746792890
     253 |          313 |          3 | 50 01:08:30.673227 |        142387 |  0.032928620648309943
     316 |          408 |          6 | 47 03:55:20.770402 |         67488 |  0.016561794401058296
     160 |          198 |          3 | 46 10:58:57.279839 |        257331 |  0.064109372434021094
     234 |          295 |          3 | 46 03:14:58.849287 |        298967 |  0.075002404933705223
     336 |          434 |          3 | 42 21:30:11.411737 |        119650 |  0.032283641354372528
     260 |          326 |          3 | 42 14:10:49.900737 |         81498 |  0.022147098984574775
     120 |          152 |          6 | 42 04:34:55.780733 |        357402 |  0.098044718864523312
     174 |          216 |          3 | 41 21:29:29.126276 |        151540 |  0.041864548459725546
      38 |           50 |          3 | 41 18:32:43.385561 |        402242 |  0.111450205221860976
     256 |          321 |          3 | 41 05:42:03.217824 |        361732 |  0.101526745844644443
     310 |          402 |          3 | 40 18:09:17.849577 |         91262 |  0.025916707105176137
      10 |           17 |         34 | 39 16:21:17.629538  | 2507360 |  0.731333341188484291
     197 |          249 |         16 | 39 01:01:00.843312 |        115296 |  0.034179390612081412
     145 |          180 |          3 | 38 23:32:38.367512 |         97918 |  0.029073399761866599
     191 |          238 |          4 | 38 19:50:04.968432 |         80786 |  0.024082120178150444
      75 |           97 |          1 | 38 01:53:06.255298 |         46614 |  0.014168448249574162
     166 |          206 |          9 | 37 16:49:21.924017 |         95327 |  0.029265093110206839
     322 |          415 |          6 | 37 14:51:51.534464 |         77523 |  0.023850944495012570
      29 |           38 |          1 | 36 19:13:28.811095 |         66107 |  0.020790922383069488
     334 |          432 |         20 | 36 09:48:30.587967 |        916268 |  0.291275365097131462
      40 |           53 |         15 | 36 04:20:33.265986 |        183134 |  0.058583509648684644
      95 |          124 |         64 | 35 23:06:59.730715 |         14995 |  0.004825857615338170
      75 |          100 |          1 | 35 21:15:44.928283 |         45085 |  0.014540992323232318
      40 |           57 |         15 | 35 20:55:19.392336 |        182493 |  0.058881637191464961
      75 |          103 |          1 | 35 20:46:57.514724 |         44735 |  0.014436151786106184
     292 |          368 |          6 | 35 06:40:57.633971 |        129236 |  0.042399460744983270
     272 |          342 |          1 | 34 23:59:03.633597 |           259 |  0.000085649744632283
      75 |          101 |          1 | 34 21:12:59.479774 |         42939 |  0.014246613252728494
     303 |          383 |          6 | 34 11:56:20.827863 |        123635 |  0.041480170188386780
     219 |          275 |          5 | 34 07:34:13.092605 |        217540 |  0.073372944022957806
      10 |           61 |          4 | 34 02:46:35.112141 |         68261 |  0.023158200975037678
     338 |          436 |          5 | 33 20:58:52.844291 |        149994 |  0.051249638412533684
      11 |           14 |          8 | 33 20:43:58.8482 |        420143 |  0.143597450781841731
     342 |          441 |          7 | 33 14:35:44.403237 |         15473 |  0.005328637046274184
     251 |          311 |          8 | 32 23:56:24.10695 |        406343 |  0.142527276461989188
     122 |          155 |          2 | 31 19:52:48.682896 |         85227 |  0.030991989301582591
      15 |           22 |          8 | 30 00:44:23.311394 |        349655 |  0.134759295537325628
     295 |          372 |          8 | 28 00:56:10.551931 |        311981 |  0.128780975956024866
     303 |          382 |          6 | 27 07:34:30.097332 |        106741 |  0.045227893917501866
      94 |          122 |          4 | 27 00:15:24.848334 |         22848 |  0.009790357254974054
      79 |          105 |          7 | 26 13:14:13.659553 |          8125 |  0.003541765453552281
     298 |          377 |          8 | 26 05:02:07.101437 |        118722 |  0.052426839989975227
     309 |          400 |         18 | 25 21:39:03.938896 |        504892 |  0.225605293870349694
     252 |          312 |          8 | 25 18:43:19.344954 |        337397 |  0.151475756138811240
     332 |          430 |         20 | 25 15:46:52.481101  | 2412385 |  1.088222400661452040
     258 |          324 |          7 | 25 00:11:36.075085 |        808556 |  0.374210889408956822
     309 |          398 |         18 | 24 20:48:39.25953 |        504558 |  0.234839877632921356
     254 |          314 |          3 | 23 23:42:46.720938 |        324574 |  0.156604849784090256
      91 |          119 |          7 | 23 21:37:40.775738 |        162790 |  0.078830609690808257
     327 |          422 |          1 | 23 18:44:35.136728 |          7706 |  0.003750471236183615
     300 |          379 |          3 | 22 23:45:54.33627 |          9332 |  0.004698054032758194
     309 |          394 |         18 | 22 17:37:08.305667 |        407781 |  0.207603667467529114
      25 |           33 |         10 | 22 07:29:38.445905 |        169292 |  0.087817145356932074
      76 |           98 |         15 | 21 02:29:51.462089 |         51010 |  0.027975342136109111
     221 |          277 |          3 | 20 21:57:10.253263 |        151942 |  0.084083816375312205
     309 |          391 |         18 | 18 20:13:01.973737 |        407528 |  0.250327096106922881
     326 |          421 |          3 | 18 16:58:19.435744 |        147595 |  0.091316619146167328
     342 |          440 |          5 | 17 14:09:12.068699 |         16806 |  0.011058382710008058
     341 |          439 |          5 | 16 08:09:11.884167 |         56276 |  0.039862528700080671
     113 |          147 |          8 | 15 21:36:56.983167 |        507511 |  0.369416746348598169
     347 |          445 |         12 | 14 01:45:08.468001 |        791880 |  0.651266128035016476
     249 |          309 |          3 | 13 22:49:17.911078 |         82585 |  0.068514919295747532
     309 |          395 |         18 | 12 20:13:50.789806 |        399900 |  0.360390143887333631
     268 |          335 |         16 | 12 18:46:39.211761  | 1861058 |  1.685131590262984043
     287 |          356 |          1 | 12 07:15:08.795003 |        121975 |  0.114755847889710737
     290 |          364 |          3 | 11 21:10:09.669269 |        117610 |  0.114561554912827284
     126 |          160 |          5 | 11 17:21:19.544236 |        259764 |  0.256460900487368543
      46 |           60 |          3 | 11 11:11:51.761338 |        336731 |  0.339887960495421906
     127 |          161 |          5 | 11 07:48:31.143797 |        274532 |  0.280560933557394283
      13 |           18 |          8 | 11 05:25:08.098439 |        487787 |  0.502920844547085892
     302 |          381 |          8 | 11 02:24:04.362189 |        115783 |  0.120727470558012110
      36 |           48 |          8 | 10 00:51:57.812461 |        363437 |  0.419132204156336779
     138 |          173 |          3 | 9 21:50:22.90227 |        185996 |  0.217228480465649014
       3 |           51 |          8 | 9 16:54:37.228757 |        358320 |  0.427346131428269366
       4 |           32 |          8 | 9 03:14:06.039634 |        426103 |  0.539886142726288913
       4 |           39 |          8 | 9 02:39:48.710005 |        418844 |  0.532075720442356981
     230 |          289 |          5 | 8 22:12:21.730303 |        364904 |  0.473199654046241934
      79 |          106 |          5 | 8 21:46:28.67315 |           812 |  0.001055109084020697
     248 |          308 |          4 | 8 21:11:14.601086 |        222645 |  0.290100805531480163
     195 |          246 |          1 | 8 20:19:57.072993 |         56801 |  0.074308238488663284
      31 |           41 |          8 | 8 18:08:43.52615 |        468034 |  0.618664170804914763
     173 |          318 |          4 | 8 14:01:39.930022 |        440615 |  0.594060997129837478
     173 |          215 |          4 | 8 09:41:43.400588 |        422959 |  0.582505190937663903
       3 |            2 |          8 | 8 08:56:56.171903 |        437254 |  0.604429396221224719
     340 |          438 |          5 | 8 07:32:03.459415 |        521460 |  0.725940372913165217
     167 |          208 |          1 | 8 05:16:11.852154 |         29799 |  0.041960266250510474
      80 |          107 |          5 | 7 22:30:09.746706 |        476568 |  0.694898260470917583
     245 |          305 |          1 | 7 20:23:51.667134 |         92929 |  0.137016604359819393
     173 |          319 |          4 | 7 17:58:40.448452 |        402075 |  0.600541777222247164
      32 |           45 |          8 | 7 17:47:04.418144 |          3780 |  0.005651707529592848
      73 |           94 |          6 | 7 14:56:00.378254 |        617921 |  0.938290581097902298
      73 |          315 |          6 | 7 07:19:08.975617 |        616056 |  0.976086508573914145
     276 |          347 |          1 | 7 06:28:45.62119 |         76219 |  0.121343561588207725
     186 |          231 |         22 | 7 02:32:23.20408    | 1302341 |  2.121272768140777691
     309 |          392 |         36 | 7 02:15:34.093295   | 1026044 |  1.673987482869832291
     349 |          448 |         12 | 7 00:06:14.480963   | 1196010 |  1.976306069774814785
     173 |          320 |          4 | 6 21:28:59.818755 |        404953 |  0.679748083393663972
     250 |          310 |          3 | 6 21:07:58.98186 |        321751 |  0.541231918735475948
      71 |           92 |          3 | 6 19:06:48.303109 |        328341 |  0.559155921776964391
     306 |          386 |          4 | 6 13:14:36.851207 |          6457 |  0.011406578428763267
     181 |          225 |          5 | 6 09:44:56.522095 |         92973 |  0.167973955189627143
     182 |          226 |          5 | 6 05:40:10.220921 |        467836 |  0.868276030844993578
     309 |          399 |         36 | 6 01:32:44.579209 |        812948 |  1.551532359739397073
     313 |          405 |         20 | 5 22:38:03.886458 |         13037 |  0.025389306936053875
     294 |          371 |          3 | 5 22:34:41.120098 |        384914 |  0.749908743821531840
     211 |          267 |          6 | 5 22:22:52.250314 |        430963 |  0.840784883957322205
     195 |          244 |          1 | 5 21:38:26.52337 |         78444 |  0.153839961649361395
      62 |           82 |          4 | 5 21:01:20.11745 |          3533 |  0.006959106489625242
     167 |          209 |          1 | 5 20:02:30.121111 |         26794 |  0.053146868121252911
     312 |          404 |         20 | 5 19:22:48.785358 |         12761 |  0.025432032386979458
     106 |          137 |          1 | 5 18:56:24.613556 |         77774 |  0.155490588659006255
      19 |           26 |          2 | 5 18:21:27.559496 |         14158 |  0.028424721176184483
     309 |          396 |         36 | 5 04:45:30.965025 |        816824 |  1.818676652487171718
     246 |          306 |          3 | 5 03:24:15.456321 |        366331 |  0.824595387153342875
     318 |          412 |          4 | 5 01:01:44.962546 |           386 |  0.000885920595772989
      49 |           64 |          6 | 4 23:44:16.633446 |        874901 |  2.029666016286006105
     309 |          397 |         36 | 4 23:21:39.346869 |        644561 |  1.500027879252289513
     173 |          317 |          4 | 4 21:56:40.930482 |        421173 |  0.991926700494724141
     105 |          136 |          6 | 4 21:39:35.491746 |        554287 |  1.308590819821044009
     299 |          378 |          3 | 4 20:22:33.355937 |        120608 |  0.287879302769295387
     171 |          214 |          5 | 4 17:18:03.424295 |        372127 |  0.912336657571210067
     296 |          373 |          4 | 4 14:26:46.420674 |        290417 |  0.730413255167513306
     222 |          278 |          5 | 4 12:57:45.757707 |        539844 |  1.376220048254205441
     200 |          253 |          5 | 4 09:51:47.690506 |        214351 |  0.562442074352801200
     293 |          369 |          3 | 4 08:55:27.975832 |        389649 |  1.031559812697860771
      20 |           27 |          4 | 4 06:56:02.891038 |        617083 |  1.665258488974602094
     178 |          221 |          5 | 4 04:37:06.139616 |        275580 |  0.760795453061850959
     136 |          171 |          3 | 3 21:58:24.801541 |        152460 |  0.450658693892415812
     343 |          443 |          5 | 3 21:34:34.228581 |         96229 |  0.285652602175420312
      57 |           75 |          8 | 3 19:12:39.925262 |        256767 |  0.781968139976656248
      72 |           93 |          3 | 3 18:12:27.152609 |         81026 |  0.249504882026037065
       4 |            8 |          8 | 3 13:13:23.475536 |        918381 |  2.993385255481690692
     226 |          285 |          4 | 3 11:03:15.891044 |          4208 |  0.014073772001705381
     227 |          286 |          5 | 3 10:43:23.547918 |        263770 |  0.885718124730430969
     151 |          187 |          3 | 3 08:19:28.504463 |        252477 |  0.873113759290148451
     176 |          227 |         22 | 3 06:57:21.762628   | 1212271 |  4.264929223600944493
     348 |          447 |         22 | 3 06:06:12.215201   | 1133534 |  4.031458084112887630
     148 |          183 |          3 | 3 05:53:02.11284 |        231422 |  0.825380755055729681
     177 |          219 |          5 | 3 05:33:55.573258 |        234944 |  0.841382769604799763
     351 |          450 |         22 | 3 04:01:10.049125   | 1239725 |  4.529998821441180607
     151 |          186 |          3 | 3 03:55:39.880284 |        249045 |  0.911118420558472363
      65 |           85 |         22 | 3 02:22:10.333466   | 1150971 |  4.298993636991700022
       4 |            3 |          8 | 3 01:54:44.725097 |        929085 |  3.491688595282221504
     146 |          181 |          3 | 3 01:09:22.837431 |         82937 |  0.314915349519383725
     217 |          273 |          5 | 3 00:27:17.531244 |        281228 |  1.078173062974307070
      17 |           24 |          8 | 2 23:23:31.233463 |        931118 |  3.622868881853937130
     176 |          446 |         22 | 2 22:36:06.556484   | 1191077 |  4.686206621660624756
     202 |          255 |          8 | 2 22:27:43.511198 |        234355 |  0.923881400573500233
     102 |          132 |          3 | 2 22:02:40.96152 |         95691 |  0.379483800439150546
     350 |          449 |         22 | 2 21:57:34.709042   | 1272824 |  5.053802665995577188
     309 |          393 |         36 | 2 21:55:34.241718 |        268610 |  1.067037992792830758
     225 |          282 |          5 | 2 21:03:17.722111 |        395282 |  1.590046749597748972
     176 |          223 |         22 | 2 20:53:08.118859   | 1122984 |  4.528378235081904594
     214 |          270 |          5 | 2 20:08:53.700503 |        228399 |  0.930972791474309016
     213 |          269 |          5 | 2 20:08:53.700503 |        228399 |  0.930972791474309016
      26 |           36 |          8 | 2 20:07:55.222068 |        917018 |  3.738730689012550616
      43 |           58 |          1 | 2 17:48:32.459701 |        122038 |  0.515118538526932872
      33 |           46 |          8 | 2 16:40:32.598405 |        705939 |  3.031959462875797218
       4 |           43 |          8 | 2 14:47:08.613331 |        666945 |  2.950710488248294601
     144 |          179 |          3 | 2 11:30:24.405581 |        288851 |  1.348357108129694748
     150 |          184 |          3 | 2 07:36:10.730715 |        254718 |  1.272503722647960760
      51 |           68 |          4 | 2 04:55:44.137834 |        430296 |  2.258248429426200660
     162 |          202 |          3 | 2 03:13:38.472587 |        266915 |  1.447333318922712589
     297 |          375 |          3 | 2 01:59:19.529406 |        146483 |  0.813977456395349605
     297 |          374 |          3 | 2 01:01:45.926452 |        141679 |  0.802686928693745434
     228 |          287 |          5 | 2 00:59:43.188898 |        221385 |  1.255136622617838798
     225 |          284 |          5 | 2 00:59:43.188898 |        221385 |  1.255136622617838798
     297 |          376 |          3 | 2 00:16:07.658941 |        140919 |  0.810962182829698873
     162 |          201 |          3 | 1 22:37:13.860713 |        309941 |  1.846713164335811104
     184 |          229 |          5 | 1 22:08:13.258843 |        157886 |  0.950586442218236391
     183 |          228 |          5 | 1 22:08:13.258843 |        157886 |  0.950586442218236391
      93 |          123 |          3 | 1 21:58:06.611578 |        220548 |  1.332724127329463859
     293 |          370 |          3 | 1 21:25:51.195464 |        261329 |  1.597842187937552060
     259 |          325 |          4 | 1 21:05:11.02186 |         37726 |  0.232430303054466889
     273 |          343 |          6 | 1 20:13:52.826867 |        669085 |  4.201928792979707190
     216 |          272 |          3 | 1 19:57:20.537635 |        142528 |  0.900704725414654649
     142 |          177 |          1 | 1 19:09:58.318578 |        151365 |  0.974045288167159077
     286 |          355 |         39 | 1 18:08:12.975523   | 4942046 | 32.579267319142782928
     328 |          423 |          3 | 1 17:24:28.7215 |         24800 |  0.166366221903902221
     308 |          388 |         18 | 1 17:12:43.914846   | 2114195 | 14.250062100305923873
      93 |          121 |          3 | 1 16:50:48.524364 |        256663 |  1.745430640056361579
     165 |          204 |          5 | 1 16:13:07.344592 |        306356 |  2.115903160343802764
     165 |          205 |          5 | 1 15:57:45.527635 |        297623 |  2.068758269563343683
     274 |          344 |          6 | 1 13:28:35.434956 |        663316 |  4.916531605270571085
      56 |           74 |          4 | 1 13:20:32.381362 |        473265 |  3.520468768053660420
     175 |          217 |          5 | 1 13:02:30.560572 |         75946 |  0.569521415389884755
      39 |           52 |          3 | 1 09:11:02.079177 |        366473 |  3.067693133458846931
      93 |          125 |          3 | 1 07:04:54.556121 |        176466 |  1.577074042897797823
     165 |          207 |          5 | 1 06:13:40.592725 |        306659 |  2.818023614105434014
     309 |          401 |         36 | 1 04:12:29.688897 |        542366 |  5.340892777624478555
     272 |          341 |          6 | 1 03:56:41.333426 |        589419 |  5.858958126370789124
     267 |          334 |          6 | 1 03:48:01.326573 |          3531 |  0.035281306922170587
     271 |          338 |          6 | 1 03:38:24.190918 |        748154 |  7.518818987398663007
     189 |          235 |          6 | 1 03:12:24.550018 |        618220 |  6.311938743772727555
     143 |          178 |          1 | 1 02:24:06.936948 |        119725 |  1.259640803211799679
     180 |          224 |          5 | 1 02:06:59.32085 |        245819 |  2.614558345855143454
     187 |          232 |          5 | 1 00:11:56.915044 |         40924 |  0.469759517762200156
     128 |          162 |          1 | 23:52:08.135562 |         85414 |  0.994016679651695292
     179 |          222 |          5 | 23:50:24.842903 |        300246 |  3.498357699755310905
     229 |          288 |          5 | 23:45:58.066637 |        272409 |  3.183907850042457710
     194 |          242 |          1 | 23:20:45.077827 |         52305 |  0.622344595928218605
     192 |          239 |          3 | 22:20:20.73815 |         57833 |  0.719130429916353609
     189 |          234 |          6 | 22:04:50.92755 |        588833 |  7.407549743706569694
     232 |          290 |          5 | 20:48:13.141075 |        311491 |  4.159139215272925445
     185 |          230 |          5 | 20:43:49.638226 |        311995 |  4.180577682223106105
     344 |          444 |         12 | 20:21:19.207434 |         97089 |  1.324918805753250554
     140 |          175 |          1 | 20:03:01.438817 |        134207 |  1.859300703886660237
     243 |          303 |          1 | 19:45:48.287556 |         73642 |  1.035049507580027525
     170 |          212 |          6 | 19:37:21.049259 |          3850 |  0.054500889219301793
     176 |          218 |          5 | 19:31:21.443567 |        252537 |  3.593224429991310056
      85 |          112 |          1 | 17:49:08.945536 |        116713 |  1.819406367864634201
     225 |          281 |          5 | 17:34:01.311289 |        202209 |  3.197419469623989504
     225 |          283 |          5 | 17:34:01.311289 |        202209 |  3.197419469623989504
     208 |          264 |          5 | 17:28:32.222614 |        533271 |  8.476429187248742844
      27 |           35 |          6 | 17:07:17.164789 |        612576 |  9.938419492476763214
      27 |          236 |          6 | 16:35:31.756313 |        561808 |  9.405516172269796289
     141 |          176 |          1 | 16:14:34.981381 |        100844 |  1.724566602987696982
     240 |          300 |          5 | 15:43:03.912188 |        336840 |  5.952928791506133178
     240 |          301 |          5 | 15:05:48.942809 |        296807 |  5.461136586282406412
     190 |          237 |          5 | 15:04:21.4842 |        309858 |  5.710459353782291123
     220 |          276 |          5 | 13:32:39.605712 |         24791 |  0.508433151540001116
     215 |          271 |          5 | 13:19:34.72786 |        338383 |  7.053359447654821986
     190 |          262 |          5 | 13:18:20.344319 |        322735 |  6.737634240177788230
     190 |          245 |          5 | 13:17:06.9288 |        323919 |  6.772732603311128771
     329 |          424 |          3 | 11:53:59.092169 |        318867 |  7.443365016748518203
     122 |          154 |          1 | 11:36:50.651886 |         65815 |  1.574120398300646577
     212 |          268 |          5 | 10:34:30.34229 |        326149 |  8.567009918523115018
     190 |          265 |          5 | 09:20:39.085333 |        345847 | 10.281105939010877445
     207 |          263 |          5 | 08:49:13.565616 |        376983 | 11.872147038820914253
     203 |          257 |          5 | 08:37:52.492357 |        243410 |  7.833616859677646102
     190 |          241 |          5 | 08:06:06.555495 |        313718 | 10.756086712185826453
     134 |          169 |          1 | 07:36:23.246559 |         83766 |  3.059023692443392428
     188 |          233 |          5 | 07:26:49.957635 |        320287 | 11.946568672748296195
     274 |          345 |          6 | 07:18:05.06465 |        628118 | 23.896384063107107481
     223 |          279 |          5 | 07:00:41.090069 |        543923 | 21.549108953421246159
      21 |           28 |          3 | 06:42:44.991517 |         96872 |  4.008774426088700869
     131 |          165 |          1 | 06:37:13.14338 |         77114 |  3.235578235337247403
     129 |          163 |          1 | 04:50:15.860167 |         94173 |  5.407312593060509039
     132 |          166 |          1 | 03:45:15.947162 |         62347 |  4.612847272390069440
     130 |          164 |          1 | 03:13:58.493265 |         63293 |  5.438246906954755195
     244 |          304 |          1 | 02:44:58.652282 |         46277 |  4.675080877843487421
      55 |           72 |          1 | 02:36:43.85809 |         99260 | 10.555242226119131068
     283 |          352 |          3 | 02:27:03.596666 |        211277 | 23.944544157839229140
     122 |          156 |          1 | 02:17:37.530357 |         89825 | 10.877949715783285252
     209 |          266 |          5 | 02:17:02.240211 |         18448 |  2.243670766918196037
      26 |           34 |          8 | 01:59:07.775667 |           333 |  0.046587919866791757
      53 |           71 |          1 | 01:24:04.837708 |        166073 | 32.919393965170544194
     135 |          170 |          1 | 01:17:35.703355 |         77719 | 16.693288655629993419
     272 |          340 |          4 | 01:09:38.686412 |           352 |  0.084236998255996435
      22 |           29 |          1 | 01:02:57.478684 |         79680 | 21.093434712813855285
     272 |          339 |          4 | 00:48:29.943993 |           117 |  0.040206959405902215
     124 |          158 |          1 | 00:10:18.299586 |         17982 | 29.082988905640315276
(361 rows)

********************************************
* License
********************************************



