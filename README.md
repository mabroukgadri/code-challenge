# README
main.py is for local executions.
lambda_handler.py is to deploy to AWS lambda along with the pyatlan and boto3 libraries.

The entry point of the script is the `lambda_handler` function
Based on the requested operation in the request payload, it will perform different actions on an Atlan instance.

The main operations are:
- create the s3 connection
- create the s3 assets and lineage.
The other operations are:
- retreive assets by guid or qualified name
- purge assets

The Connection qualified name is a mandatory input for the s3 connection creation operation. The goal is to avoid creating redundant connections having the same name on the Atlan instance.

Possible improvements:
- add mock metadata to the assets
- follow the openAPI standard and create a separate endpoint for each operation
- use an input validation library like pydantic instead of the `validate_parameters` function
- use type hints
- add unit tests
- reorganize code in multiple modules/classes
- use secret manager to store the api key on AWS
- automate the lambda function and scheduling deployment using terraform

# Prerequistes to run the code
- generate an atlan api key
- create the follwing environment variables
    - ATLAN_BASE_URL='https://instance-name.atlan.com'
    - ATLAN_API_KEY='xxxxxxxxxxxxxxx'
- for local executions configure aws cli with credentials that can access s3
- install python dependencies from requirements.txt
- run main.py after adapting the code at the end with the request type to be passed to the lambda_handler function


# Examples of all the supported requests
```python
upsert_s3_connection_request = {
    "operation": "upsert_s3_connection",
    "params": {
        "connection_qn": "default/s3/1720796029",
    }
}

upsert_s3_assets_and_lineage_request = {
    "operation": "upsert_s3_assets_and_lineage",
    "params": {
        "s3_connection_qualified_name": "default/s3/1720796029",
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
    },
}


get_by_guid_request = {
    "operation": "get_by_guid",
    "params": {
        "guid": "82c11978-e115-4d4f-88c4-06be25a99127",
        "asset_type": "S3Bucket"
    }
}

get_by_qn_request = {
    "operation": "get_by_qn",
    "params": {
        "qualified_name": "default/s3/1720796029/arn:aws:s3:::atlan-tech-challenge-mag",
        "asset_type": "S3Bucket"
    }
}

purge_request = {
    "operation": "purge",
    "params": {
        "assets_guids": [
            "c2e074f9-c904-4816-b51d-fd10203bf7c8",
            "e87ab78e-16f1-4004-ac00-5ae9f2b91b48"
        ]
    }
}

# print(
#     json.dumps(
#         lambda_handler(purge_request, None),
#         indent=4,
#     )
# )
```
