# Copy Dynamo DB Table
Copies one Dynamo DB Table to another

This script will copy one DynamoDB table to another one along with it's metadata.  
This uses `multiprocessing.Process` to have multiple parallel scanners on the source table in order to
**significantly improve the performance** (and decrease the runtime).

## Prerequisites

* This script should be run on an environment where `awscli` is already configured.
    * Otherwise, you need to export these environment variables.  
    `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_DEFAULT_REGION`  
* Python3 (3.7+) to run this script.
* IAM Role corresponding to `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` **MUST** have the privileges to  
read from the source table and create new tables.

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

or

```shell script
pip3 install -r requirements.txt
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

or

```shell script
python3 copy_dynamodb_table.py [-h]
```

Perform copy:
```shell script
python copy_dynamodb_table.py -s <source_table_name> -t <target_table_name> [-c] [-v]
```

or

```shell script
python3 copy_dynamodb_table.py -s <source_table_name> -t <target_table_name> [-c] [-v]
```

**Param/Flag** | **Purpose** |
| ------------- |:-------------|
| `-s` or `--source` | Name of the source DynamoDB table (Required) |
| `-t` or `--target` | Name of the target DynamoDB table (Required) |
| `-n` or `--num-threads` | Number of parallel threads/processes to scan the source table (default=`5`) |
| `-c` or `--create-table` | Whether to create the target table if it does not exist (`False` if not passed) |
| `-v` or `--verbose-copy` | Whether to copy additional information (i.e. Tags, Encryption, Stream) (`False` if not passed) |

Example:

```shell script
python copy_dynamodb_table.py -n 10 -c -v -s prod_table -t dev_table
```

or

```shell script
python3 copy_dynamodb_table.py -n 10 -c -v -s prod_table -t dev_table
```

## Note
It's advised to run this script on a powerful computer because otherwise, it will take a lot of time to finish for a larger table.  
Running on a powerful AWS EC2 instance will benefit a lot since it will reduce the network overhead.