# YCSB-TS
[![Build Status](https://travis-ci.org/TSDBBench/YCSB-TS.svg?branch=master)](https://travis-ci.org/TSDBBench/YCSB-TS)

YCSB-TS is a fork of [YCSB](http://github.com/brianfrankcooper/YCSB) that is adopted to use basic time domain functions and support timestamps and timeranges.
It is used in [TSDBBench](https://tsdbbench.github.io/) to measure the performance of time series databases (TSDBs).
To achieve this, many new workload options are introduced, as well as bindings for many TSDBs.

The benchmark is usually executed using [Overlord](https://github.com/TSDBBench/Overlord).

Supported databases are tracked at <http://tsdbbench.github.io/Overlord/#supported-databases>.

## Getting Started

* To build the full distribution, with all database bindings:
    `mvn clean package`
* The usage is the same as original YCSB, see the documentation [here](http://github.com/brianfrankcooper/YCSB).
    * new workload options are documented in [workload_template](workloads/workload_template) and [CoreWorkload.java](core/src/main/java/com/yahoo/ycsb/workloads/CoreWorkload.java)

## Additional Information

* Everything was tested and used on Debian Jessie x64, but should work on Ubuntu.
    * Ubuntu has different package names for a lot of the packages, you need to find and change them
* Logfiles/Benchmark Results are stored compressed as .ydc Files 
    
## Development Information

* See the [README.md of Overlord](https://github.com/TSDBBench/Overlord) for more information
* The original tests are untouched and therefore not working and should be fixed
* [Adding a New Database](adding_a_database.md)

## Funding

TSDBBench received funding from the
[Federal Ministry for Economic Affairs and Energy](http://www.bmwi.de/Navigation/EN/Home/home.html)
in the context of the project [NEMAR](https://www.nemar.de/).

![BMWi](https://tsdbbench.github.io/BMWi.jpg)

## Related Links

* [YCSB on Github](http://github.com/brianfrankcooper/YCSB)
* [YCSB Wiki on Github](http://wiki.github.com/brianfrankcooper/YCSB)
* [Overlord](https://github.com/TSDBBench/Overlord)
* [Comparison of TSDBs (Andreas Bader)](http://www2.informatik.uni-stuttgart.de/cgi-bin/NCSTRL/NCSTRL_view.pl?id=DIP-3729&mod=0&engl=0&inst=FAK)
* [Survey and Comparison of Open Source TSDBs (Andreas Bader, Oliver Kopp, Michael Falkenthal)](http://www2.informatik.uni-stuttgart.de/cgi-bin/NCSTRL/NCSTRL_view.pl?id=INPROC-2017-06&mod=0&engl=0&inst=IPVS)
* [Ultimate Comparison of TSDBs](https://tsdbbench.github.io/Ultimate-TSDB-Comparison/)
