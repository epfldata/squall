/*  Sccsid:     @(#)README	9.1.1.14     1/23/96  09:43:27 */

Table of Contents
===================
 0. What is this document?
 1. What is DBGEN?
 2. What will DBGEN create?
 3. How is DBGEN built?
 4. Command Line Options for DBGEN
 5. Building Large Data Sets with DBGEN
 6. DBGEN limitations and compliant usage
 7. Sample DBGEN executions
 8. What is QGEN?
 9. What will QGEN create?
10. How is QGEN built?
11. Command Line Options for QGEN
12. Query Template Syntax
13. Sample QGEN executions and Query Templates
14. Environment variable
15. Version Numbering in DBGEN and QGEN

0. What is this document?

This is the general README file for DBGEN and QGEN, the data-
base population and executable query text generation programs 
used in the TPC-D benchmark. It covers the proper use of DBGEN and 
QGEN. For information on porting the utility to your particular 
platform see Porting.Notes.

1. What is DBGEN?

DBGEN is a database population program for use with the TPC-D benchmark.
It is written in ANSI 'C' for portability, and has been successfully
ported to over a dozen different systems. While the TPC-D specification
allows an implementor to use any utility to populate the benchmark
database, the resultant population must exactly match the output of
DBGEN. The source code has been provided to make the process of building
a compliant database population as simple as possible.

2. What will DBGEN create?

Without any command line options, DBGEN will generate 8 separate ascii
files. Each file will contain pipe-delimited load data for one of the
tables defined in the TPC-D database schema. The default tables will
contain the load data required for a scale factor 1 database. By 
default the file will be created in the current directory and be 
named <table>.tbl. As an example, customer.tbl will contain the 
load data for the customer table.

When invoked with the '-U' flag, DBGEN will create the data sets to be 
used in the update functions and the SQL syntax required to delete the 
data sets. The update files will be created in the same directory as 
the load data files and will be named "u_<table>.set". The delete 
syntax will be written to "delete.set". For instance, the data set to 
be used in the third query set to update the lineitem table will be 
named "u_lineitem.tbl.3", and the SQL to remove those rows will be 
found in "delete.3". The size of the update files can be controlled 
with the '-r' flag.

3. How is DBGEN built?

Create an appropriate makefile, using makefile.suite as a basis, 
and type make.  Refer to Porting.Notes for more details and for 
suggested compile time options.

4. Command Line Options for DBGEN

DBGEN's output is controlled by a combination of command line options
and environment variables. Command line options are assumed to be single
letter flags preceded by a minus sign. They may be followed by an
optional argument.

option  argument    default     action
------  --------    -------     ------
-v      none                    Verbose. Progress messages are 
                                displayed as data is generated.

-f      none                    Force. Existing data files will be
                                overwritten.

-F      none        yes         Flat file output.

-D      none                    Direct database load. ld_XXXX() routines
                                must be defined in load_stub.c

-s      <scale>     1           Scale of the database population. Scale
                                1.0 represents ~1 GB of data

-T      <table>                 Generate the data for a particular table
                                ONLY. Arguments: p -- part/partuspp, 
                                c -- customer, s -- supplier, 
                                o -- order/lineitem, t -- time, 
                                n -- nation, r -- region,
                                l -- code (same as n and r),
                                O -- order, L -- lineitem, P -- part, 
                                S -- partsupp

-O      d                       Generate SQL for delete function 
                                instead of key ranges

-O      f                       Allow over-ride of default output file 
                                names

-O      h                       Generate headers in flat ascii files.
                                hd_XXX routines must be defined in 
                                load_stub.c

-O      m                       Flat files generate fixed length records

-O      r                       Generate key ranges for the UF2 update 
                                function

-O      s                       Generate the state files for the random
                                number generator.

-O      t                       Generate the optional time table and its
                                associated join fields

-h                              Display a usage summary

-U      <updates>               Create a specified number of data sets
                                in flat files for the update/delete 
                                functions

-r      <percentage>     10     Scale each udpate file to the given 
                                percentage (expressed in basis points)
                                of the data set

-n      <name>                  Use database <name> for in-line load

-C      <children>              Use <children> separate processes to 
                                generate data

-S      <n>                     Generate the <n>th part of a multi-part load

5. Building Large Data Sets with DBGEN

DBGEN relies on its own random number generator to assure that identical data
sets can be generated on different platforms. In order to build large data sets
using either parallel or multi-stage loads, it is important that the random 
number generator be started, or "seeded", correctly for each step in the load. 
DBGEN includes an option to create correct seed files,  ascii data files used 
to seed the randowm number generator. 

Each line in a seed file represents the state of one part of the random 
number generator after a stage of the data generation has been completed.  
Seed files are named based on the size of the data set being constructed, the
number of children or steps involved in the build, and which step a particular
seed file represents. The naming convention for seed files is mncccsss, where
SF=m * 10^n, ccc is the number of children in the load (in hex) and sss is the
number of the current stage (in hex). For example, the 10th seed file in a 30
process load of 300 GB would be 3201E00A. Since there can be a large number of
seed files, DBGEN allows you to segregate them in the directory named in the
environment variable DSS_SEED.

6. DBGEN limitations and compliant usage

DBGEN is meant to be a robust population generator for use with the 
TPC-D benchmark. It is hoped that DBGEN will make it easier to experi-
ment with and become proficient in the execution of TPC-D's. As a 
result, it includes a number of command line options which are not, 
strictly speaking, necessary to generate a compliant data set for a 
TPC-D run. In addition, some command line options will accept arguments 
which result in the generation of NON-COMPLIANT data sets. Options 
which should be used with care include:

-s -- scale factor. TPC-D runs are only compliant when run against SF's 
      of 1, 10, 30, 100, 300, 1000 ....
-r -- refresh percentage. TPC-D runs are only compliant when run with 
      -r 10, the default.

7. Sample DBGEN executions

DBGEN has been built to allow as much flexibility as possible, but is
fundementally intended to generate two things: a database population 
against which the queries in TPC-D can be run, and the updates that are
used during the update functions in TPC-D. Here are some sample uses
of DBGEN.

  1. To generate the database population for the qualification database
	dbgen -s 0.1
  2. To generate the lineitem table only, for a scale factor 10 database,
     and over-write any existing flat files:
	dbgen -s 10 -f -T L
  3. To build the seed files necessary to load a 30GB data set in 10 steps,
     and include some progress reports:
        dbgen -v -O s -s 30 -C 10
  4. To geterate a 100GB data set in 1GB pieces, generate only the part and 
     partsupplier tables, and include some progress reports along the way:
	dbgen -s 100 -S 1 -C 100 -T p -v (to generate the first 1GB file)
	dbgen -s 100 -S 2 -C 100 -T p -v (to generate the second 1GB file)
        (and so on, incrementing the argument to -S each time)
  5. To generate the update files needed for a 4 stream run of the throughput
     test at 100 GB, using an existing set of seed files from an 8 process 
     load:
	dbgen -s 100 -U 4 -C 8
     Note: since the state of the seed files for a given scale factor is the
     same at the end of the load regardless of the number of children used
     in the load (the same data has been generated, resulting in the same
     modifications to the RNG), the -C argument is arbitrary in the generation
     of updates. Use whatever seed files are available.
     

8. What is QGEN?

QGEN is a query generation program for use with the TPC-D benchmark.
It is written in ANSI 'C' for portability, and has been successfully
ported to over a dozen different systems. While the TPC-D specification
allows an implementor to use any utility to create the benchmark query
sets, QGEN has been provided to make the process of building
a benchmark implementation as simple as possible.

9. What will QGEN create?

QGEN is a filter, triggered by :'s. It does line-at-a-time reads of its
input (more on that later), scanning for :foo, where foo determines the
substitution that occurs. Including:

:<int>          replace with the appropriate value for parameter <int>
:b              replace with START_TRAN (from tpcd.h)
:c              replace with SET_DBASE (from tpcd.h)
:n<int>         replace with SET_ROWCOUNT(<int>) (from tpcd.h)
:o              replace with SET_OUTPUT (from tpcd.h)
:q              replace with query number
:s              replace with stream number
:x              replace with GEN_QUERY_PLAN (from tpcd.h)

Qgen takes an assortment of command line options, controlling which of these
options should be active during the translation from template to EQT, and a
list of query "names". It then translates the template found in
$DSS_QUERY/<name>.sql and puts the result of stdout.

Here is a sample query template:

{  Sccsid:     @(#)1.sql        9.1.1.1     1/25/95  10:51:56  }
:n 0
:o
select
 l_returnflag,
 l_linestatus,
 sum(l_quantity) as sum_qty,
 sum(l_extendedprice) as sum_base_price,
 sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
 sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
 avg(l_quantity) as avg_qty,
 avg(l_extendedprice) as avg_price,
 avg(l_discount) as avg_disc,
 count(*) as count_order
from lineitem
where l_shipdate <= date '1998-12-01' - interval :1 day
group by l_returnflag, l_linestatus
order by l_returnflag, l_linestatus;

And here is what is generated:
$ qgen -d 1

{return 0 rows}

select
 l_returnflag,
 l_linestatus,
 sum(l_quantity) as sum_qty,
 sum(l_extendedprice) as sum_base_price,
 sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
 sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
 avg(l_quantity) as avg_qty,
 avg(l_extendedprice) as avg_price,
 avg(l_discount) as avg_disc,
 count(*) as count_order
from lineitem
where l_shipdate <= date('1998-12-01') - interval (90)  day to day
group by l_returnflag, l_linestatus
order by l_returnflag, l_linestatus;

See "Query Template Syntax" below for more detail on converting your prefered query
phrasing for use with QGEN.

10. How is QGEN built?

QGEN is built by the same makefile that creates DBGEN. If the makefile
is successfully creating DBGEN, no further compilation modifications
should be necessary. You may need to modify some of the options which
allow QGEN to integrate with your preferred query tool. Refer to
Porting.Notes for more detail.

11. Command Line Options for QGEN

Like DBGEN, QGEN is controlled by a combination of command line options
and environment variables (See "Environment Variables", below for more
detail).  Command line options are assumed to be single
letter flags preceded by a minus sign. They may be followed by an
optional argument.

option  argument    default     action
------  --------    -------     ------
-c      none                    Retain comments in translation of template to
                                EQT

-d      none                    Default. Use the parameter substitutions
                                required for query validation

-h                              Display a usage summary

-i      <file>                  Use contents of <file> to init a query stream

-l      <file>                  Save query parameters to <file>

-n      <name>                  Use database <name> for queries

-N                              Always use default rowcount, and ignore :n directives

-o      <path>                  Save query n's output in <path>/n.<stream>
                                Uses -p option, and uses :o tag

-p      <stream>                Use the query permutation defined for
                                stream <stream>. If this option is
                                omited, EQT will be generated for the
                                queries named on the command line.

-r      <n>                     Seed the rnadom number generator with <n>

-s      <n>                     Set scale to <n> for parameter 
                                substitutions.

-t      <file>                  Use contents of <file> to complete a query 
                                stream

-T      none                    Use time table format for date substitution

-v      none                    Verbose. Progress messages are 
                                displayed as data is generated.

-x      none                    Generate a query plan as part of query
                                execution.

12. Query Template Syntax

QGEN is a simple ASCII text filter, meant to translate query generalized
query syntax("query template") into the executable query text(EQT) re-
quired by TPC-D. It provides a number of shorthands and syntactic exten-
sions that allow the automatic generation of query parameters and some 
control over the operation of the benchmark implementation.

QGEN first strips all comments from the query template, recognizing both
{comment} and --comment styles. Next it traverses the query template
one line at a time, locating required substitution points, called
parameter tags. The values substituted for a given tag are summarized
below.  QGEN does not support nested substitutions. That is, if
the text substituted for tag itself contains a valid tag the second tag
will not be expanded.

Tag             Converted To            Based on
===             ============            ========
:c		database <dbname>;(1)   -n from the command line
:x              set explain on;(1)      -x from the command line
:<number>       paremeter <number>
:s              stream number
:o              output to outpath/qnum.stream;(1)
					-o from command line, -s from 
                                        command line
:b              BEGIN WORK;(1)          -a from comand line
:e              COMMIT WORK(1)          -a from command line
:q              query number
:n <number>                             sets rowcount to be returned 
                                        to <number>, unless -N appears on the command line

Notes:
   (1)  This is Informix-specific syntax. Refer to Porting.Notes for
   tailoring the generated text to your database environment.
   
13. Sample QGEN executions and Query Templates

QGEN translates generic query templates into valid SQL. In addition, it 
allows conditional inclusion of the commands necessary to connect to a 
database, produce diagnostic output, etc. Here are some sample of QGEN
usage, and the way that command line parameters and the query templates 
interact to produce valid SQL.

  Template, in $DSS_QUERY/1.sql:
            :c
            :o
            select count(*) from foo;
            :x
            select count(*) from lineitem
              where l_orderdate < ':1';

  1. "qgen 1", would produce:
      select count(*) from foo;
      select count(*) from lineitem 
        where l_orderdate < '1997-01-01'; 
   Assuming that 1 January 1997 was a valid substitution for parameter 1.

  2. "qgen -d -c dss1 1, would produce:
      database dss1;
      select count(*) from foo;
      select count(*) from lineitem 
        where l_orderdate < '1995-07-18'; 
   Assuming that 18 July 1995 was the default substitution for parameter 1,
    and using Informix syntax.

  3. "qgen -d -c dss1 -x -o somepath 1, would produce:
      database dss1;
      output to "somepath/1.0"
      select count(*) from foo;
      set explain on;
      select count(*) from lineitem 
        where l_orderdate < '1995-07-18'; 
   Assuming that 18 July 1995 was the default substitution for parameter 1,
    and using Informix syntax.
 

14. Environment Variables

Enviroment variables are used to control features of DBGEN and QGEN 
which are unlikely to change from one execution to another.

Variable    Default     Action
-------     -------     ------
DSS_PATH    .           Directory in which to build flat files
DSS_CONFIG  .           Directory in which to find configuration files
DSS_DIST    dists.dss   Name of distribution definition file
DSS_QUERY   .           Directory in which to find query templates
DSS_SEED    .           Directory in which to find seed files

15. Version Numbering in DBGEN and QGEN

DBGEN and QGEN use a common version numbering algorithm. Each executable
is stamped with a version number which is displayed in the usage messages
available with the '-h' option. A version number is of the form:

   V.R.P.M
   | | | |
   | | | |
   | | | |
   | | |  -- modification: alphabetic, incremented for any trivial changes 
   | | |                   to the source (e.g, porting ifdef's)
   | |  ---- patch level:  numeric, incremented for any minor bug fix
   | |                     (e.g, qgen parameter range)
   | ------- release:      numeric, incremented for each major revision of the
   |                       specification
   |-------- version:      numeric, incremented for each minor revision of the 
                           specification

An implementation of TPC-D is valid only if it conforms to the following
version usage rules:
  -- The Version of DBGEN and QGEN must match the fractional portion of the 
     current specification revision
  -- The Release of DBGEN and QGEN must match the integer portion of the 
     current specification revision
  -- The Modification of DBGEN and QGEN must be the most recent release as
     announced in the minutes from the last General Council meeting of the TPC
  -- The Patch Level must be disclosed as part of the Full Disclosure Report,
     but all unaltered DBGEN/QGEN sources whose version numbers differ only 
     in Patch Level are comparable.

The current revisions are:
  DBGEN: 1.3.1
  QGEN:  1.3.1
