import json
from lambda_function import lambda_handler


if __name__ == "__main__":

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

    print(
        json.dumps(
            lambda_handler(upsert_request_with_table_regex, None),
            indent=4,
        )
    )
