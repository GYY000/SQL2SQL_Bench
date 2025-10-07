# SQL2SQL_Bench

<p align="center">
    <img src="figures/system-overview.svg" width="1000px">
</p>

## Installation

We have developed *SQL2SQL_Bench* based on Ubuntu 24.04,  MySQL v8.0, PostgreSQL v14 (with PostGIS v3.5.0 extension for spatial types), and Oracle 11g.

Clone this repository and install necessary dependencies:

```bash
conda env create -f environment.yml
conda activate sql2sqlbench-py310
```

## Quick Start

To get started with *SQL2SQL_Bench*, follow these steps:

### Step 1: Set up SQL2SQL_Bench
First, download the data used in our paper in Google Drive: **tbd** and write the file path to `data_path` in `src/config.ini`.

```ini
[FILE_PATH]
data_path = your data path
```

Run the python script to install the database:

```bash
cd src
python -m db_builder.create_db_script.py
```

Second, set your API keys in `src/config.ini`:

```ini
[API]
gpt_api_base = xxx
gpt_api_key = xxx
deepseek_api_base = xxx
deepseek_api_key = xxx
...
```

Third, set the database connection information in `src/config.ini`:

```ini
[ORACLE_CONN]
oracle_instant_path = your oracle instant client path
oracle_host = your oracle host
oracle_port = your oracle port
oracle_user = your oracle user
...
```

### Step 2: Generate SQL with certain translation points

### Step 3: Transpile SQL to target dialects using different dialect translators

### Step 4: Verify the correctness and efficiency of the translated SQL

## Code Structure
- `conv_point/`: The collected translation points.
- `exp_data/`: The experiment data.
- `src/`: The source code of *SQL2SQL_Bench*.
  - `antlr_parser`: The antlr parser and their syntax definition file used by *SQL2SQL_Bench*.
  - `db_builder`: .
  - `models`: .
  - `sql_gen`: .
  - `transpiler`: .
  - `utils`: .
  - `verification`: .
  - `config.ini`: The configuration file for *SQL2SQL_Bench*.
