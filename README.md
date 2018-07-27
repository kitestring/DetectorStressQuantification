# DetectorStressQuantification

### Application Dependencies
1. Python 3.6.4
1. Linux cinnamon 18.3 or Windows10 64 bit
1. pandas==0.22.0
1. numpy==1.14.0
1. matplotlib==2.1.2
1. psycopg2==2.7.4
1. os
1. pprint
1. datetime
1. re
1. math

### Application Description
ETL flat data exported from a Chemical Analyzer instrument (TOF-MS).  While the data is still in memory data is cleansed and linked so that primary and foreign keys can be generated prior to loading into a Postgres managed database using a Snowflake schema.  A CLI is implemented to query the DB to generate customizable data visualizations.

### Command Line Interface (CLI) Example
![CLI](https://github.com/kitestring/DetectorStressQuantification/blob/master/ExampleOutput/CLI_Example.png)

### To maintain confidentiality the data examples shown below are not “real” and are simulated data sets.

### Detector Measurement Ion Statistics Plot examle
![Data Set #1 Example](https://github.com/kitestring/DetectorStressQuantification/blob/master/ExampleOutput/DM_API_Analysis_Upgraded.png)

### System Performance vs Detector Voltage Offset
![Data Set #2 Example](https://github.com/kitestring/DetectorStressQuantification/blob/master/ExampleOutput/Eicosane_5_0pg_plots.png)
