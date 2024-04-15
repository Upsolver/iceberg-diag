# Iceberg Diagnostics Tool

## Overview

The Iceberg Table Analysis CLI Tool evaluates Iceberg tables to identify how Upsolver optimizations can enhance efficiency.   
It presents a side-by-side comparison of current metrics against potential improvements in scan durations, file counts, and file sizes, providing a straightforward assessment of optimization opportunities.


## Installation
`iceberg-diag` can be installed using either Brew or PIP, as detailed below:

### Using PIP
#### Prerequisites

* **Python 3.8 or higher**: Verify Python's installation:
   ```bash
   python3 --version
   ```
To install `iceberg-diag` using PIP, ensure you have the latest version of `pip`:

```bash
pip install --upgrade pip
```
Then, install the package with `pip`
```bash
pip install iceberg-diag
```


### Using Brew
Execute the following commands to install `iceberg-diag` via Brew:

 ```bash
 brew tap upsolver/iceberg-diag
 brew install iceberg-diag
 ```


## Usage Instructions

```bash
iceberg-diag [options]
```

### Command-Line Options

- `-h`, `--help`: Display the help message and exit.
- `--profile PROFILE`: Set the AWS credentials profile for the session, defaults to the environment's current settings.
- `--region REGION`: Set the AWS region for operations, defaults to the specified profile's default region.
- `--database DATABASE`: Set the database name, will list all available iceberg tables if no `--table-name` provided.
- `--table-name TABLE_NAME`: Enter the table name or a glob pattern (e.g., `'*'`, `'tbl_*'`).
- `--remote`: Enable remote diagnostics by sending data to the Upsolver API for processing.   
Provides more detailed analytics and includes information about file size reducations.
- `-v, --verbose`: Enable verbose logging

### Usage
1. Displaying help information:
    ```bash
     iceberg-diag --help
    ```
   
2. Listing all available databases in profile:
    ```bash
   iceberg-diag --profile <profile>
    ```
   
3. Listing all available iceberg tables in a given database:
    ```bash
   iceberg-diag --profile <profile> --database <database>
    ```
4. Running diagnostics on a specific table in a specific AWS profile and region (completely locally):
   ```bash
    iceberg-diag --profile <profile> --region <region> --database <database> --table-name '*'
    ```
   
5. Running diagnostics using `remote` option
    ```bash
   iceberg-diag --profile <profile> --database <database> --table-name 'prod_*' --remote
    ```