# Kebab Storm
## Introduction
A Spark driver for to demonstrate how to apply cryptography (with AES) on UDF level with data quality checks and transformations based on ETL scenarios in JSON format. Additionally, the solution has both row based soft and hard delete features.
## Setup (Spark & Hadoop on localhost)
### Windows
Please use this [link](https://phoenixnap.com/kb/install-spark-on-windows-10).
### Mac
Please use this [link](https://sparkbyexamples.com/spark/install-apache-spark-on-mac/).
## External Dependencies (requirements.txt)
```sh
certifi==2022.9.24
charset-normalizer==3.0.0
confuse==2.0.0
dateutils==0.6.12
decorator==5.1.1
docopt==0.6.2
idna==3.4
numpy==1.23.4
pipreqs==0.4.11
protobuf==4.21.9
py4j==0.10.9.7
pycryptodome==3.15.0
pyspark==3.3.1
python-dateutil==2.8.2
pytz==2022.6
PyYAML==6.0
requests==2.28.1
six==1.16.0
urllib3==1.26.12
validators==0.20.0
yarg==0.1.9
```
## Driver Settings
All necessary Spark and solution specific settings are located in [/conf](https://github.com/egulay/kebab_storm/tree/master/conf) directory per environment.
For to set the default environment please refer [global.yml](https://github.com/egulay/kebab_storm/blob/master/conf/global.yml)

## Features
Executor scripts are located in [/etl/executor/](https://github.com/egulay/kebab_storm/tree/master/etl/executor) directory with the features below:
* [Encrypt & Import](https://github.com/egulay/kebab_storm/blob/master/etl/executor/import.py)
* [Print Imported](https://github.com/egulay/kebab_storm/blob/master/etl/executor/print_imported.py)
* [Print Sample Data with Schema](https://github.com/egulay/kebab_storm/blob/master/etl/executor/print_sample_data_with_schema.py)
* [Print Soft Deleted](https://github.com/egulay/kebab_storm/blob/master/etl/executor/print_soft_deleted.py)
* [Refine & Print](https://github.com/egulay/kebab_storm/blob/master/etl/executor/refine_and_print.py)
* [Soft Delete](https://github.com/egulay/kebab_storm/blob/master/etl/executor/soft_delete.py)
* [Hard Delete](https://github.com/egulay/kebab_storm/blob/master/etl/executor/hard_delete.py)
### Scenario
An example JSON based scenario located in [/scenario](https://github.com/egulay/kebab_storm/blob/master/scenario/sales_records_scenario.json) directory.
```json
{
  "entity_name": "Sales",
  "import_save_location": "/Sales",
  "report_save_location": "/Sales_Reports",
  "temp_save_location": "/Users/emregulay/tmp/Sales",
  "import_save_type": "parquet",
  "report_save_type": "parquet",
  "import_mode": "append",
  "id_field": "Order_ID",
  "delimiter": ",",
  "hard_delete_condition_in_years": 3,
  "enforce_data_model": true,
  "is_apply_year_to_save_location": true,
  "fields": [
    {
      "name": "Region",
      "map_as": "REGION",
      "type": "string",
      "encryption_key": "6tzm3NpUfDgnz7SBvLXKJVFvHGnp7q",
      "is_encrypted": true
    },
    {
      "name": "Country",
      "map_as": "COUNTRY",
      "type": "string",
      "encryption_key": "Y8RCFHrTv5GbPCEA4zp65M5BYfWfqk",
      "is_encrypted": true
    },
    {
      "name": "Item_Type",
      "map_as": "ITEMTYPE",
      "type": "string",
      "encryption_key": "M6NgJPgfJ3xzkjsP5eMHzUJpjXSspE",
      "is_encrypted": true
    },
    {
      "name": "Sales_Channel",
      "map_as": "SALESCHANNEL",
      "type": "string",
      "encryption_key": "CEY64MuGcnUyXpWtyA885thG9NPPks",
      "is_encrypted": false
    },
    {
      "name": "Order_Priority",
      "map_as": "ORDERPRIORITY",
      "type": "string",
      "encryption_key": "GLF75Jet46dNnW2eyLD5AXs4JGLTHc",
      "is_encrypted": false
    },
    {
      "name": "Order_Date",
      "map_as": "ORDERDATE",
      "type": "timestamp|%m/%d/%Y|%Y-%m-%d",
      "encryption_key": "PYHW7Nqtu7SNUcvXWZgadKqH2EQ7tGj8",
      "is_encrypted": true
    },
    {
      "name": "Order_ID",
      "map_as": "ORDERID",
      "type": "int",
      "encryption_key": "n8ZddZeeLfA2qmpRMkXZ7unmdWqUMLmx",
      "is_encrypted": true
    },
    {
      "name": "Ship_Date",
      "map_as": "SHIPDATE",
      "type": "timestamp|%m/%d/%Y|%Y-%m-%d",
      "encryption_key": "nLp8pXzWHpbKyAtn45HN7FGrdRM9jTf7",
      "is_encrypted": true
    },
    {
      "name": "Units_Sold",
      "map_as": "UNITSOLD",
      "type": "int",
      "encryption_key": "CGvFGagy3qDham7W8gwjPzVaBtcf5EPc",
      "is_encrypted": true
    },
    {
      "name": "Unit_Price",
      "map_as": "UNITPRICE",
      "type": "double",
      "encryption_key": "fUGjtwUwdYdzR2wrV7a5nDY4PptuRZB8",
      "is_encrypted": false
    },
    {
      "name": "Unit_Cost",
      "map_as": "UNITCOST",
      "type": "double",
      "encryption_key": "egR6r5UKNj4URcM4Af2YMYC9QXyzw2dP",
      "is_encrypted": true
    },
    {
      "name": "Total_Revenue",
      "map_as": "TOTALREVENUE",
      "type": "double",
      "encryption_key": "JpM2qSZH2u5JmCaqY7WnMmjN4vrKHtff",
      "is_encrypted": true
    },
    {
      "name": "Total_Cost",
      "map_as": "TOTALCOST",
      "type": "double",
      "encryption_key": "W57xBz8DgZte8CpPpHMu2NN2uEyGzAfd",
      "is_encrypted": true
    },
    {
      "name": "Total_Profit",
      "map_as": "TOTALPROFIT",
      "type": "double",
      "encryption_key": "3t2NqvPThFZRhQyguqPYcruREKCjqYfJ",
      "is_encrypted": true
    }
  ]
}
```
### Execution
#### Help
```sh
import.py --help
```
```sh
 ____  __.    ___.         ___.     _________ __
|    |/ _|____\_ |__ _____ \_ |__  /   _____//  |_  ___________  _____
|      <_/ __ \| __ \\__  \ | __ \ \_____  \\   __\/  _ \_  __ \/     \
|    |  \  ___/| \_\ \/ __ \| \_\ \/        \|  | (  <_> )  | \/  Y Y  \
|____|__ \___  >___  (____  /___  /_______  /|__|  \____/|__|  |__|_|  /
        \/   \/    \/     \/    \/        \/                         \/
usage: import.py [-h] --scenario /path/to/scenario.json OR
                 https://domain.com/scenario-id --input-file
                 /path/to/input_file.csv --date 2020-01-01

KebabStorm: A Spark driver for to demonstrate how to apply cryptography (with
AES) on UDF level with data quality checks based on ETL scenarios in JSON
format

optional arguments:
  -h, --help            show this help message and exit

required arguments:
  --scenario /path/to/scenario.json OR https://domain.com/scenario-id, -scn /path/to/scenario.json OR https://domain.com/scenario-id
                        Scenario JSON file path or URL
  --input-file /path/to/input_file.csv, -if /path/to/input_file.csv
                        File to import
  --date 2020-01-01, -d 2020-01-01
                        Import date in YYYY-mm-dd format
```
#### Encrypt & Import
```sh
import.py --scenario ../../scenario/sales_records_scenario.json --input-file ../../data/50k_sales_records_corrupted.csv --date 2020-02-23
```

```sh
 ____  __.    ___.         ___.     _________ __
|    |/ _|____\_ |__ _____ \_ |__  /   _____//  |_  ___________  _____
|      <_/ __ \| __ \\__  \ | __ \ \_____  \\   __\/  _ \_  __ \/     \
|    |  \  ___/| \_\ \/ __ \| \_\ \/        \|  | (  <_> )  | \/  Y Y  \
|____|__ \___  >___  (____  /___  /_______  /|__|  \____/|__|  |__|_|  /
        \/   \/    \/     \/    \/        \/                         \/
2022-05-20 09:28:53,256 - etl.executor - INFO - ###### KEBAB STORM STARTED | Active YAML Configuration: local on Spark 3.0.1 with application ID local-1653028131680 ######
2022-05-20 09:28:53,265 - etl.importer - INFO - Active action: Encrypt & Import for Sales entity defined in ../../scenario/sales_records_scenario.json on day=20200223
2022-05-20 09:28:59,561 - etl.importer - INFO - Raw data loaded from ../../data/50k_sales_records_corrupted.csv for Sales entity
2022-05-20 09:28:59,562 - etl.refiner - INFO - Refinement will be applied to following column(s): Region, Country, Item_Type, Sales_Channel, Order_Priority, Order_Date, Order_ID, Ship_Date, Units_Sold, Unit_Price, Unit_Cost, Total_Revenue, Total_Cost, Total_Profit
2022-05-20 09:29:00,481 - etl.importer - INFO - Data refinement and validation applied for Sales entity
2022-05-20 09:29:00,482 - etl.crypter - INFO - Encryption will be applied to following mapped column(s): REGION, COUNTRY, ITEMTYPE, ORDERDATE, ORDERID, SHIPDATE, UNITSOLD, UNITCOST, TOTALREVENUE, TOTALCOST, TOTALPROFIT
2022-05-20 09:29:00,953 - etl.importer - INFO - Cryptography applied for Sales entity
2022-05-20 09:29:00,953 - etl.importer - INFO - Save as parquet started. Import mode: append
2022-05-20 09:29:04,229 - etl.refiner - WARNING - On column Total_Profit with Order_ID: 897751939 has corrupted value. 59c7290.92 cannot be parsable to Spark double. Null value is assigned for that cell.
2022-05-20 09:29:04,230 - etl.refiner - WARNING - On column Total_Cost with Order_ID: 599480426 has corrupted value. 13c44707.70 cannot be parsable to Spark double. Null value is assigned for that cell.
2022-05-20 09:29:04,232 - etl.refiner - WARNING - On column Total_Revenue with Order_ID: 538911855 has corrupted value. 204g5322.72 cannot be parsable to Spark double. Null value is assigned for that cell.
2022-05-20 09:29:04,233 - etl.refiner - WARNING - On column Ship_Date with Order_ID: 459845054 has corrupted value. 3/2j/2012 cannot be parsable to Spark timestamp. Null value is assigned for that cell.
2022-05-20 09:29:31,770 - etl.importer - INFO - Save as parquet finished with partition day=20200223 Import mode: append
2022-05-20 09:29:31,771 - etl.executor - INFO - C:/Users/emreg/PycharmProjects/kebab_storm/etl/executor/import.py executed in 38.53 seconds
```
#### Print Imported Data
##### Encrypted
```sh
print_imported.py --scenario ../../scenario/sales_records_scenario.json --crypto-action decrypted --date 2020-02-23
```
```sh
 ____  __.    ___.         ___.     _________ __
|    |/ _|____\_ |__ _____ \_ |__  /   _____//  |_  ___________  _____
|      <_/ __ \| __ \\__  \ | __ \ \_____  \\   __\/  _ \_  __ \/     \
|    |  \  ___/| \_\ \/ __ \| \_\ \/        \|  | (  <_> )  | \/  Y Y  \
|____|__ \___  >___  (____  /___  /_______  /|__|  \____/|__|  |__|_|  /
        \/   \/    \/     \/    \/        \/                         \/
2022-05-20 09:36:19,436 - etl.executor - INFO - ###### KEBAB STORM STARTED | Active YAML Configuration: local on Spark 3.0.1 with application ID local-1653028577542 ######
2022-05-20 09:36:19,447 - etl.printer - INFO - Active action: Print imported data for Sales entity defined in ../../scenario/sales_records_scenario.json on day=20200223
root
 |-- REGION: binary (nullable = true)
 |-- COUNTRY: binary (nullable = true)
 |-- ITEMTYPE: binary (nullable = true)
 |-- SALESCHANNEL: string (nullable = true)
 |-- ORDERPRIORITY: string (nullable = true)
 |-- ORDERDATE: binary (nullable = true)
 |-- ORDERID: binary (nullable = true)
 |-- SHIPDATE: binary (nullable = true)
 |-- UNITSOLD: binary (nullable = true)
 |-- UNITPRICE: double (nullable = true)
 |-- UNITCOST: binary (nullable = true)
 |-- TOTALREVENUE: binary (nullable = true)
 |-- TOTALCOST: binary (nullable = true)
 |-- TOTALPROFIT: binary (nullable = true)
 |-- DATE_IMPORTED: string (nullable = true)
 |-- SOFT_DELETED: timestamp (nullable = true)
 |-- YEAR_IMPORTED: integer (nullable = true)

+--------------------+--------------------+--------------------+------------+-------------+--------------------+--------------------+--------------------+--------------------+---------+--------------------+--------------------+--------------------+--------------------+-------------+------------+-------------+
|              REGION|             COUNTRY|            ITEMTYPE|SALESCHANNEL|ORDERPRIORITY|           ORDERDATE|             ORDERID|            SHIPDATE|            UNITSOLD|UNITPRICE|            UNITCOST|        TOTALREVENUE|           TOTALCOST|         TOTALPROFIT|DATE_IMPORTED|SOFT_DELETED|YEAR_IMPORTED|
+--------------------+--------------------+--------------------+------------+-------------+--------------------+--------------------+--------------------+--------------------+---------+--------------------+--------------------+--------------------+--------------------+-------------+------------+-------------+
|[2F 50 37 6B 55 3...|[42 37 5A 43 70 5...|[4F 47 4B 4F 6D 5...|     Offline|            M|[38 6A 75 36 5A 7...|[76 6A 6D 58 49 4...|[77 45 46 6F 43 4...|[41 49 55 65 73 5...|   668.27|[31 6F 51 48 78 4...|[4D 6E 56 56 5A 4...|[6D 69 79 67 4A 6...|                null|     20200223|        null|         2020|
|[50 70 62 6C 47 6...|[75 49 6B 62 48 7...|[67 6C 32 7A 70 7...|      Online|            H|[6A 6B 6B 49 76 4...|[34 63 57 50 6C 5...|[58 2B 75 4B 66 6...|[64 43 52 56 33 6...|   255.28|[54 71 67 77 72 7...|[2B 55 2B 51 2B 7...|                null|[74 48 33 6D 70 4...|     20200223|        null|         2020|
|[54 75 56 71 2F 7...|[39 4F 46 59 70 3...|[73 58 30 59 49 5...|      Online|            L|[32 41 34 62 51 6...|[65 55 78 65 50 3...|[59 6B 4C 7A 6E 4...|[48 62 77 61 33 4...|   421.89|[39 49 44 41 6B 3...|                null|[2B 31 4F 39 53 7...|[43 73 7A 6D 39 3...|     20200223|        null|         2020|
|[74 77 4C 43 4D 7...|[4F 4A 4A 4A 48 2...|[33 43 76 71 38 3...|      Online|            L|[2B 6B 43 64 4B 5...|[64 2F 79 58 56 4...|                null|[49 44 6F 46 69 3...|   421.89|[73 56 71 51 75 4...|[58 2B 2F 6D 45 5...|[59 45 4C 64 32 4...|[55 52 38 73 44 4...|     20200223|        null|         2020|
|[57 4E 79 31 71 4...|[6E 7A 2B 6F 66 4...|[36 31 34 76 6E 7...|      Online|            M|[4E 76 45 6E 71 6...|[62 77 2F 71 61 7...|[38 50 4E 43 36 4...|[73 71 42 52 65 7...|    205.7|[74 74 47 48 67 7...|[53 4B 69 54 4E 6...|[51 67 5A 4E 62 4...|[49 76 32 72 46 6...|     20200223|        null|         2020|
|[48 2B 5A 50 51 4...|[57 52 65 6F 75 3...|[64 73 74 79 64 4...|      Online|            H|[79 62 79 43 37 3...|[32 32 42 66 70 5...|[56 32 35 32 61 5...|[6B 4C 72 38 58 6...|   421.89|[56 63 49 56 51 5...|[33 78 33 41 66 4...|[41 54 52 41 44 4...|[57 58 6F 53 6E 4...|     20200223|        null|         2020|
|[75 66 36 56 6A 3...|[35 61 47 6A 43 6...|[31 74 4E 6C 76 4...|      Online|            M|[5A 59 44 39 79 6...|[50 63 6C 38 71 5...|[2B 6D 77 55 34 5...|[70 64 65 65 58 6...|   668.27|[6D 4F 67 62 4C 4...|[55 47 59 46 43 7...|[6A 55 75 68 6E 6...|[34 50 72 48 59 6...|     20200223|        null|         2020|
|[77 47 72 71 5A 4...|[33 36 52 78 55 3...|[79 48 43 39 34 5...|      Online|            L|[30 39 35 41 46 3...|[4E 4B 61 56 49 6...|[35 68 6E 49 65 6...|[30 4F 39 6B 72 7...|   668.27|[4D 4B 48 5A 65 6...|[62 42 70 77 53 5...|[65 6E 51 32 6E 3...|[47 6A 34 67 30 4...|     20200223|        null|         2020|
|[70 56 58 43 7A 4...|[50 68 71 56 6F 3...|[62 70 4E 6E 6D 6...|     Offline|            M|[41 61 54 78 64 7...|[37 53 33 69 78 6...|[48 56 50 58 67 5...|[5A 31 4C 47 32 4...|    437.2|[42 66 42 64 52 2...|[4D 36 55 2F 51 6...|[50 74 47 37 51 4...|[48 78 57 57 4E 6...|     20200223|        null|         2020|
|[71 36 30 37 6E 6...|[49 51 38 32 2F 7...|[63 6F 4B 6D 39 4...|      Online|            C|[6B 4B 77 4B 65 5...|[56 71 41 50 6E 6...|[33 48 6E 50 6F 5...|[51 4F 42 59 6D 6...|    437.2|[45 72 44 73 62 5...|[73 75 46 56 47 6...|[75 72 38 70 6B 4...|[37 6E 30 4C 53 4...|     20200223|        null|         2020|
|[6F 50 47 48 54 4...|[79 50 4D 31 70 3...|[39 6D 61 33 50 4...|     Offline|            M|[67 50 79 42 34 2...|[59 68 48 6F 76 7...|[38 62 67 32 44 7...|[4A 69 54 43 41 7...|     9.33|[72 41 4D 44 49 4...|[69 42 56 63 45 4...|[4E 35 4E 35 77 5...|[31 34 4B 55 66 6...|     20200223|        null|         2020|
|[50 54 6B 6C 57 4...|[4F 33 45 4B 6A 4...|[5A 31 74 61 77 3...|     Offline|            L|[71 76 4E 33 35 5...|[59 4A 4F 2B 2B 7...|[55 57 58 47 68 7...|[32 6E 4B 67 54 7...|    437.2|[59 72 4D 51 72 7...|[63 36 6C 71 71 5...|[65 72 6E 74 7A 4...|[35 32 30 78 2B 6...|     20200223|        null|         2020|
|[74 4F 47 7A 32 4...|[55 42 4C 48 6A 6...|[4A 6B 46 4A 4E 7...|      Online|            C|[78 61 77 30 38 6...|[38 30 49 35 58 6...|[4C 6A 58 2B 4B 3...|[65 52 2F 70 43 6...|   154.06|[69 45 68 77 37 4...|[69 67 33 4E 74 3...|[39 44 42 74 7A 5...|[32 45 48 6F 67 4...|     20200223|        null|         2020|
|[76 2B 6B 61 45 5...|[64 54 4C 67 69 3...|[69 38 73 56 37 4...|      Online|            L|[54 6B 53 2F 54 7...|[57 71 31 78 76 6...|[46 50 44 54 67 5...|[6D 6C 37 4B 32 4...|   154.06|[75 51 69 75 4A 7...|[6E 67 61 38 47 5...|[51 61 54 55 6B 6...|[47 44 69 77 76 3...|     20200223|        null|         2020|
|[68 69 50 6D 43 4...|[30 65 65 47 45 4...|[52 51 30 47 38 3...|      Online|            L|[58 75 5A 54 66 4...|[62 69 33 57 75 7...|[42 43 78 33 68 4...|[6A 50 56 2F 47 7...|   651.21|[53 4A 68 6A 43 5...|[48 49 32 37 71 7...|[30 2B 6C 4F 52 3...|[6E 35 69 49 62 3...|     20200223|        null|         2020|
|[64 38 39 4A 53 5...|[79 62 74 4E 5A 4...|[6A 68 77 45 4F 6...|     Offline|            M|[4E 55 72 79 37 6...|[70 32 4C 4A 6F 3...|[47 69 71 52 42 7...|[4C 79 48 50 43 7...|    437.2|[39 7A 47 6E 74 6...|[6E 63 47 2F 55 4...|[69 79 33 56 66 5...|[6A 67 79 55 35 4...|     20200223|        null|         2020|
|[42 59 6E 36 4C 3...|[7A 4C 4C 37 31 7...|[77 6A 32 72 73 3...|     Offline|            M|[6A 56 36 49 4E 7...|[39 35 69 55 51 2...|[49 4B 56 38 32 4...|[68 70 4D 55 4C 4...|    81.73|[53 57 49 6D 58 6...|[78 44 47 4A 44 4...|[44 4B 64 62 50 5...|[6F 68 62 4D 72 7...|     20200223|        null|         2020|
|[61 42 43 33 77 7...|[66 6D 68 6F 79 5...|[2F 46 65 54 43 6...|     Offline|            M|[6C 76 66 72 69 5...|[68 73 70 44 43 4...|[77 52 42 43 57 4...|[56 4C 4F 4C 4E 7...|   255.28|[6C 38 61 42 79 3...|[56 34 64 63 76 4...|[4A 70 53 36 76 7...|[68 71 36 30 38 7...|     20200223|        null|         2020|
|[72 2F 56 6F 42 4...|[53 75 32 73 79 5...|[38 34 37 38 76 4...|      Online|            H|[41 6C 62 6F 68 3...|[55 69 70 33 66 3...|[65 65 59 2B 4B 2...|[34 68 53 56 50 6...|   651.21|[4D 59 5A 48 76 4...|[52 74 62 2B 78 6...|[74 4D 38 36 64 5...|[59 74 59 72 70 4...|     20200223|        null|         2020|
|[65 51 35 73 39 6...|[4D 2F 47 68 64 4...|[38 34 2B 35 4D 2...|     Offline|            H|[2F 67 44 7A 4E 5...|[2F 48 4D 33 72 6...|[37 62 57 59 45 6...|[2F 49 6A 38 48 5...|    47.45|[48 79 43 75 47 6...|[6F 62 2B 33 69 4...|[2F 35 56 78 6A 5...|[52 77 38 35 6D 4...|     20200223|        null|         2020|
+--------------------+--------------------+--------------------+------------+-------------+--------------------+--------------------+--------------------+--------------------+---------+--------------------+--------------------+--------------------+--------------------+-------------+------------+-------------+
only showing top 20 rows

Total Count: 50000
```
##### Decrypted
```sh
print_imported.py --scenario ../../scenario/sales_records_scenario.json --crypto-action decrypted --date 2020-02-23
```
```sh
 ____  __.    ___.         ___.     _________ __
|    |/ _|____\_ |__ _____ \_ |__  /   _____//  |_  ___________  _____
|      <_/ __ \| __ \\__  \ | __ \ \_____  \\   __\/  _ \_  __ \/     \
|    |  \  ___/| \_\ \/ __ \| \_\ \/        \|  | (  <_> )  | \/  Y Y  \
|____|__ \___  >___  (____  /___  /_______  /|__|  \____/|__|  |__|_|  /
        \/   \/    \/     \/    \/        \/                         \/
2022-05-20 09:37:46,894 - etl.executor - INFO - ###### KEBAB STORM STARTED | Active YAML Configuration: local on Spark 3.0.1 with application ID local-1653028665290 ######
2022-05-20 09:37:46,903 - etl.printer - INFO - Active action: Print imported data for Sales entity defined in ../../scenario/sales_records_scenario.json on day=20200223
2022-05-20 09:37:54,268 - etl.crypter - INFO - Decryption will be applied to following mapped column(s): REGION, COUNTRY, ITEMTYPE, ORDERDATE, ORDERID, SHIPDATE, UNITSOLD, UNITCOST, TOTALREVENUE, TOTALCOST, TOTALPROFIT
root
 |-- REGION: string (nullable = true)
 |-- COUNTRY: string (nullable = true)
 |-- ITEMTYPE: string (nullable = true)
 |-- SALESCHANNEL: string (nullable = true)
 |-- ORDERPRIORITY: string (nullable = true)
 |-- ORDERDATE: timestamp (nullable = true)
 |-- ORDERID: string (nullable = true)
 |-- SHIPDATE: timestamp (nullable = true)
 |-- UNITSOLD: string (nullable = true)
 |-- UNITPRICE: double (nullable = true)
 |-- UNITCOST: double (nullable = true)
 |-- TOTALREVENUE: double (nullable = true)
 |-- TOTALCOST: double (nullable = true)
 |-- TOTALPROFIT: double (nullable = true)
 |-- DATE_IMPORTED: string (nullable = true)
 |-- SOFT_DELETED: timestamp (nullable = true)
 |-- YEAR_IMPORTED: integer (nullable = true)

+--------------------+-----------+---------------+------------+-------------+-------------------+---------+-------------------+--------+---------+--------+------------+----------+-----------+-------------+------------+-------------+
|              REGION|    COUNTRY|       ITEMTYPE|SALESCHANNEL|ORDERPRIORITY|          ORDERDATE|  ORDERID|           SHIPDATE|UNITSOLD|UNITPRICE|UNITCOST|TOTALREVENUE| TOTALCOST|TOTALPROFIT|DATE_IMPORTED|SOFT_DELETED|YEAR_IMPORTED|
+--------------------+-----------+---------------+------------+-------------+-------------------+---------+-------------------+--------+---------+--------+------------+----------+-----------+-------------+------------+-------------+
|  Sub-Saharan Africa|    Namibia|      Household|     Offline|            M|2015-08-31 00:00:00|897751939|2015-10-12 00:00:00|    3604|   668.27|  502.54|  2408445.08|1811154.16|       null|     20200223|        null|         2020|
|              Europe|    Iceland|      Baby Food|      Online|            H|2010-11-20 01:00:00|599480426|2011-01-09 01:00:00|    8435|   255.28|  159.42|   2153286.8|      null|   808579.1|     20200223|        null|         2020|
|              Europe|     Russia|           Meat|      Online|            L|2017-06-22 00:00:00|538911855|2017-06-25 00:00:00|    4848|   421.89|  364.69|        null|1768017.12|   277305.6|     20200223|        null|         2020|
|              Europe|    Moldova|           Meat|      Online|            L|2012-02-28 01:00:00|459845054|               null|    7225|   421.89|  364.69|  3048155.25|2634885.25|   413270.0|     20200223|        null|         2020|
|              Europe|      Malta|         Cereal|      Online|            M|2010-08-12 00:00:00|626391351|2010-09-13 00:00:00|    1975|    205.7|  117.11|    406257.5| 231292.25|  174965.25|     20200223|        null|         2020|
|                Asia|  Indonesia|           Meat|      Online|            H|2010-08-20 00:00:00|472974574|2010-08-27 00:00:00|    2542|   421.89|  364.69|  1072444.38| 927041.98|   145402.4|     20200223|        null|         2020|
|  Sub-Saharan Africa|   Djibouti|      Household|      Online|            M|2011-02-03 01:00:00|854331052|2011-03-03 01:00:00|    4398|   668.27|  502.54|  2939051.46|2210170.92|  728880.54|     20200223|        null|         2020|
|              Europe|     Greece|      Household|      Online|            L|2015-09-11 00:00:00|895509612|2015-09-26 00:00:00|      49|   668.27|  502.54|    32745.23|  24624.46|    8120.77|     20200223|        null|         2020|
|  Sub-Saharan Africa|   Cameroon|      Cosmetics|     Offline|            M|2014-01-31 01:00:00|241871583|2014-02-04 01:00:00|    4031|    437.2|  263.33|   1762353.2|1061483.23|  700869.97|     20200223|        null|         2020|
|  Sub-Saharan Africa|    Nigeria|      Cosmetics|      Online|            C|2015-11-21 01:00:00|409090793|2015-12-07 01:00:00|    7911|    437.2|  263.33|   3458689.2|2083203.63| 1375485.57|     20200223|        null|         2020|
|  Sub-Saharan Africa|    Senegal|         Fruits|     Offline|            M|2016-08-29 00:00:00|733153569|2016-10-05 00:00:00|    5288|     9.33|    6.92|    49337.04|  36592.96|   12744.08|     20200223|        null|         2020|
|Middle East and N...|Afghanistan|      Cosmetics|     Offline|            L|2016-10-21 00:00:00|620358741|2016-12-01 00:00:00|    6792|    437.2|  263.33|   2969462.4|1788537.36| 1180925.04|     20200223|        null|         2020|
|                Asia|      India|     Vegetables|      Online|            C|2010-03-21 01:00:00|897317636|2010-04-05 00:00:00|    5084|   154.06|   90.93|   783241.04| 462288.12|  320952.92|     20200223|        null|         2020|
|Middle East and N...|    Lebanon|     Vegetables|      Online|            L|2010-10-15 00:00:00|660954082|2010-11-19 01:00:00|    9855|   154.06|   90.93|   1518261.3| 896115.15|  622146.15|     20200223|        null|         2020|
|Middle East and N...|     Turkey|Office Supplies|      Online|            L|2010-10-04 00:00:00|428504407|2010-11-13 01:00:00|    2831|   651.21|  524.96|  1843575.51|1486161.76|  357413.75|     20200223|        null|         2020|
|Middle East and N...|       Iraq|      Cosmetics|     Offline|            M|2014-10-14 00:00:00|787517440|2014-10-19 00:00:00|    2766|    437.2|  263.33|   1209295.2| 728370.78|  480924.42|     20200223|        null|         2020|
|  Sub-Saharan Africa|     Rwanda|  Personal Care|     Offline|            M|2013-06-15 00:00:00|145854508|2013-07-08 00:00:00|     445|    81.73|   56.67|    36369.85|  25218.15|    11151.7|     20200223|        null|         2020|
|              Europe|    Ukraine|      Baby Food|     Offline|            M|2017-05-07 00:00:00|581689441|2017-05-29 00:00:00|    3687|   255.28|  159.42|   941217.36| 587781.54|  353435.82|     20200223|        null|         2020|
|              Europe|    Finland|Office Supplies|      Online|            H|2015-05-21 00:00:00|193508565|2015-07-03 00:00:00|    2339|   651.21|  524.96|  1523180.19|1227881.44|  295298.75|     20200223|        null|         2020|
|  Sub-Saharan Africa|South Sudan|      Beverages|     Offline|            H|2016-06-28 00:00:00|750110709|2016-07-14 00:00:00|    3283|    47.45|   31.79|   155778.35| 104366.57|   51411.78|     20200223|        null|         2020|
+--------------------+-----------+---------------+------------+-------------+-------------------+---------+-------------------+--------+---------+--------+------------+----------+-----------+-------------+------------+-------------+
only showing top 20 rows

Total Count: 50000
```
#### Report Generation from Imported Data
##### Create
An example report implementation located in [/reporter/simple_sales_report](https://github.com/egulay/kebab_storm/blob/master/reporter/simple_sales_report/create_simple_sales_report.py) directory.
```sh
create_simple_sales_report.py --scenario ../../scenario/sales_records_scenario.json --include-soft-deleted nope --date 2020-02-23
```
**_OR_**
```sh
create_simple_sales_report.py --scenario ../../scenario/sales_records_scenario.json --include-soft-deleted nope --date 2019-02-27->2020-12-01
```
-----
```sh
 ____  __.    ___.         ___.     _________ __
|    |/ _|____\_ |__ _____ \_ |__  /   _____//  |_  ___________  _____
|      <_/ __ \| __ \\__  \ | __ \ \_____  \\   __\/  _ \_  __ \/     \
|    |  \  ___/| \_\ \/ __ \| \_\ \/        \|  | (  <_> )  | \/  Y Y  \
|____|__ \___  >___  (____  /___  /_______  /|__|  \____/|__|  |__|_|  /
        \/   \/    \/     \/    \/        \/                         \/
2022-05-20 09:49:51,882 - reporter - INFO - ###### KEBAB STORM STARTED | Active YAML Configuration: local on Spark 3.0.1 ######
2022-05-20 09:49:51,891 - etl.importer - INFO - Active action: Get reporting data for Sales entity defined in ..\..\scenario\sales_records_scenario.json on day=20200223
2022-05-20 09:49:59,286 - etl.crypter - INFO - Decryption will be applied to following mapped column(s): REGION, COUNTRY, ITEMTYPE, ORDERDATE, ORDERID, SHIPDATE, UNITSOLD, UNITCOST, TOTALREVENUE, TOTALCOST, TOTALPROFIT
2022-05-20 09:50:08,303 - reporter - INFO - Total row(s) will be processed for Simple_Report is 50000
2022-05-20 09:50:08,385 - reporter - INFO - Save as parquet started
2022-05-20 09:50:24,219 - reporter - INFO - Save as parquet finished with partition day=20220520
2022-05-20 09:50:24,220 - reporter - INFO - C:/Users/emreg/PycharmProjects/kebab_storm/reporter/simple_sales_report/create_simple_sales_report.py executed in 32.35 seconds
```
#### Print Report
An example report printer located in [reporter/simple_sales_report](https://github.com/egulay/kebab_storm/blob/master/reporter/simple_sales_report/print_simple_sales_report.py) directory.
```sh
print_simple_sales_report.py  --scenario ../../scenario/sales_records_scenario.json --date 2022-05-20
```
```sh
 ____  __.    ___.         ___.     _________ __
|    |/ _|____\_ |__ _____ \_ |__  /   _____//  |_  ___________  _____
|      <_/ __ \| __ \\__  \ | __ \ \_____  \\   __\/  _ \_  __ \/     \
|    |  \  ___/| \_\ \/ __ \| \_\ \/        \|  | (  <_> )  | \/  Y Y  \
|____|__ \___  >___  (____  /___  /_______  /|__|  \____/|__|  |__|_|  /
        \/   \/    \/     \/    \/        \/                         \/
2022-05-20 09:57:56,669 - reporter - INFO - ###### KEBAB STORM STARTED | Active YAML Configuration: local on Spark 3.0.1 ######
2022-05-20 09:57:56,678 - etl.printer - INFO - Active action: Print report data for Simple_Report based on ../../scenario/sales_records_scenario.json on day=20220520
2022-05-20 09:57:56,678 - etl.printer - INFO - Identified parquet path: C://data//Sales_Reports_2022/Simple_Report/day=20220520
root
 |-- COUNTRY: string (nullable = true)
 |-- TOTAL_SALES_ENTRY: long (nullable = true)
 |-- TOTAL_SALES_REVENUE: decimal(10,0) (nullable = true)

+--------------------+-----------------+-------------------+
|             COUNTRY|TOTAL_SALES_ENTRY|TOTAL_SALES_REVENUE|
+--------------------+-----------------+-------------------+
|Federated States ...|              247|          322487754|
|Sao Tome and Prin...|              295|          403103704|
|Saint Kitts and N...|              287|          372955948|
|Republic of the C...|              255|          377094398|
| Trinidad and Tobago|              321|          368689701|
|  Dominican Republic|              286|          356125340|
|       Liechtenstein|              254|          328421913|
|        Saudi Arabia|              294|          370408584|
|Saint Vincent and...|              251|          299529193|
|         The Bahamas|              274|          390726869|
|         North Korea|              268|          356425950|
|         Switzerland|              248|          341406542|
|         Saint Lucia|              287|          374353667|
|         New Zealand|              267|          356349810|
|          San Marino|              265|          374522598|
|          East Timor|              280|          390964302|
|          The Gambia|              281|          385652930|
|          Tajikistan|              277|          360275003|
|          Mauritania|              285|          367085931|
|          Seychelles|              288|          366996301|
+--------------------+-----------------+-------------------+
only showing top 20 rows

Total Count: 185
```
### License
The MIT License (MIT)
Copyright © 2020

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
