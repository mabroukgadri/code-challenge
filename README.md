# README
The aim of this script is to import an S3 bucket and its files witin it (that fit a prefix and a regex) as Atlan assets, then based on file names or file name/table name pattern matching, creates the lineage from each source table to the corresponding S3 file and from each s3 file to the corresponding snowflake table.
if new tables or files are added to any connection, the script will import them automatically and build lineage based on object names.

`local_execution.py` is to be run on a laptop.
`lambda_handler.py` is deployed to the Lambda service in my AWS personal account along with the pyatlan and boto3 libraries.

The entry point of both scripts is the `lambda_handler` function.

Based on the requested operation in the request payload, it will perform different actions on an Atlan instance.

The main operations are:
- create the s3 connection
- create the s3 assets and lineage. (the goal of this challenge)

The other operations are:
- retreive assets by guid or qualified name
- purge assets

The Connection qualified name is a mandatory input for the s3 connection creation operation. The goal is to avoid creating redundant connections having the same name on the Atlan instance.

Possible improvements:
- add mock metadata to the assets like tags and terms.
- follow the openAPI standard and create a separate endpoint for each operation in this script.
- use type hints
- add unit tests
- use secret manager to store the api key on AWS
- automate the lambda function and scheduling deployment using terraform

# How to run the code locally
- generate an atlan api key
- create the following environment variables
    - ATLAN_BASE_URL='https://instance-name.atlan.com'
    - ATLAN_API_KEY='xxxxxxxxxxxxxxx'
- for local executions configure aws cli with credentials that can access s3
- create a python3 environement variable and activate it. Example: `python3.11 -m venv && source venv/bin/activate`
- install python dependencies from requirements.txt: `pip install -r requirements.txt`
- run `python local_execution.py` after adapting the code at the end with the request type to be passed to the lambda_handler function
- execute with the `upsert_s3_connection_request` payload first (done only once)
- execute with `upsert_s3_assets_and_lineage` payload to import the s3 objects and create the missing lineage

# Examples of all the supported requests
```python
upsert_s3_connection_request = {
    "operation": "upsert_s3_connection",
    "params": {
        "connection_qn": "default/s3/1720796029",
        "connection_name": "aws-s3-connection-mag",
        "asset_owners": ["mag.i"],
    },
}

get_by_guid_request = {
    "operation": "get_by_guid",
    "params": {
        "guid": "82c11978-e115-4d4f-88c4-06be25a99127",
        "asset_type": "S3Bucket",
    },
}

get_by_qn_request = {
    "operation": "get_by_qn",
    "params": {
        "qualified_name": "default/s3/1720796029/arn:aws:s3:::atlan-tech-challenge-mag",
        "asset_type": "S3Bucket",
    },
}

purge_request = {
    "operation": "purge",
    "params": {
        "assets_guids": [
            "f82e4095-81b1-4793-9a0d-bde3cfe6b9c4",
            "c5247f84-2dd2-41ae-b292-338b2836fbb3",
        ]
    },
}

upsert_request = {
    "operation": "upsert_s3_assets_and_lineage",
    "params": {
        "s3_connection_qualified_name": "default/s3/1720796029",
        "asset_owners": ["mag.i"],
        "source_connection_qualified_name": "default/postgres/1720611661",
        "source_database_schema_qualified_name": "default/postgres/1720611661/FOOD_BEVERAGE/SALES_ORDERS",
        "target_connection_qualified_name": "default/snowflake/1720611799",
        "target_database_schema_qualified_name": "default/snowflake/1720611799/FOOD_BEVERAGE/SALES_ORDERS",
        "source_extraction_process_name_suffix": "pg to s3 process",
        "source_extraction_process_id_suffix": "pg_to_s3_process",
        "target_import_process_name_suffix": "s3 to snflk process",
        "target_import_process_id_suffix": "s3_to_snflk_process",
        "s3_bucket_name": "atlan-tech-challenge",
        "s3_bucket_arn": "arn:aws:s3:::atlan-tech-challenge",
        "qualifier_suffix": "mag",
        "s3_bucket_prefix": "",
    },
}

upsert_request_with_table_regex = {
    "operation": "upsert_s3_assets_and_lineage",
    "params": {
        "s3_connection_qualified_name": "default/s3/1720796029",
        "asset_owners": ["mag.i"],
        "source_connection_qualified_name": "default/postgres/1720611661",
        "source_database_schema_qualified_name": "default/postgres/1720611661/FOOD_BEVERAGE/SALES_ORDERS",
        "target_connection_qualified_name": "default/snowflake/1720611799",
        "target_database_schema_qualified_name": "default/snowflake/1720611799/FOOD_BEVERAGE/SALES_ORDERS",
        "source_extraction_process_name_suffix": "pg to s3 process",
        "source_extraction_process_id_suffix": "pg_to_s3_process",
        "target_import_process_name_suffix": "s3 to snflk process",
        "target_import_process_id_suffix": "s3_to_snflk_process",
        "s3_bucket_name": "atlan-tech-challenge",
        "s3_bucket_arn": "arn:aws:s3:::atlan-tech-challenge",
        "qualifier_suffix": "mag",
        "s3_bucket_prefix": "",
        "s3_file_name_pattern": "{table_name}\.csv",
        "source_table_pattern": "{table_name}",
        "target_table_pattern": "{table_name}",
    },
}

# print(
#     json.dumps(
#         lambda_handler(upsert_request_with_table_regex, None),
#         indent=4,
#     )
# )
```
