# Iceberg Diagnostics Tool (iceberg-diag)

## Overview

The Iceberg Diagnostics Tool, `iceberg-diag`, is a CLI tool designed for diagnostic purposes related to Iceberg tables. 

## Prerequisites

Check if Python 3.8 or higher is installed:
```bash
python3 --version
```
Check if Rust is installed:
```bash
cargo --version
```
If Rust is not installed, install it using:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Installation


### Using Brew
 ```bash
 brew tap upsolver/iceberg-diag https://github.com/Upsolver/iceberg-diag
brew install iceberg-diag
 ```


## Setup Project (dev)


1. Clone the repository from GitHub.
2. Navigate to the project directory.
3. Install dependencies using poetry:

```bash
poetry install
```


## Usage
If you are running with Poetry, use `Poetry run` before any command,
If the CLI is installed, you can use the commands as is.
```bash
iceberg-diag [options]
# for poetry:
poetry run iceberg-diag [options]
```

### Options

- `-h`, `--help`: Display the help message and exit.
- `--profile PROFILE`: Specify the AWS profile name.
- `--region REGION`: Define the AWS region.
- `--database DATABASE`: Set the database name.
- `--table-name TABLE_NAME`: Enter the table name or a glob pattern (e.g., `'*'`, `'tbl_*'`).
- `--remote`: Enable remote diagnostics.  (Note: This option is currently not supported)
- `--no-tracking`: Disable tracking functionality. (Note: This option is currently not supported)

### Examples
1. Displaying help information:
    ```bash
     poetry run iceberg-diag --help
    ```
2. Running diagnostics on a specific table in a specific AWS profile and region:
   ```bash
   poetry run iceberg-diag --help --profile default --region us-east-1 --database my_db --table-name '*'
    ```