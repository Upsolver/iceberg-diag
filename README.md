# Iceberg Diagnostics Tool (iceberg-diag)

## Overview

The Iceberg Diagnostics Tool, `iceberg-diag`, is a CLI tool designed for diagnostic purposes related to Iceberg tables. 

## Installation

1. Clone the repository from GitHub.
2. Navigate to the project directory.
3. Install dependencies using poetry:

```bash
poetry install
```


## Usage
For now - run it as a module with the required options.

```bash
poetry run upsolver-diag [options]
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
     python -m icebergdiag.cli --help
    ```
2. Running diagnostics on a specific table in a specific AWS profile and region:
   ```bash
   python -m icebergdiag.cli --profile default --region us-east-1 --database my_db --table-name '*'
    ```