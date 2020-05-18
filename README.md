# Copy Dynamo DB Table
Copies one dynamo DB to another

This script will copy one DynamoDB table to another one along with it's metadata.

## How to run

### Create a Python Virtual Environment (Optional)

```shell script
python3 -m venv .venv
source .venv/bin/activate
```

### Install Requirements

```shell script
pip install -r requirements.txt
```

### Running the script

Go to the `src` directory:
```shell script
cd src
```

Get help about the script:
```shell script
python copy_dynamodb_table.py [-h]
```

Perform copy:
```shell script
python copy_dynamodb_table.py -s <source_table_name> -t <target_table_name> [-c] [-v]
```

**Param/Flag** | **Purpose** |
| ------------- |:-------------|
| `-s` or `--source` | Name of the source DynamoDB table |
| `-t` or `--target` | Name of the target DynamoDB table |
| `-n` or `--num-threads` | Number of parallel threads/processes to scan the source table |
| `-c` or `--create-table` | Whether to create the target table if it does not exist (`False` if not passed) |
| `-v` or `--verbose-copy` | Whether to copy additional information (i.e. Tags, Encryption, Stream) (`False` if not passed) |

Example:

```shell script
python copy_dynamodb_table.py -s prod_table -t dev_table -c -v
```