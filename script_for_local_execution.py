import json
from lambda_function import lambda_handler


if __name__ == "__main__":

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
    
    upsert_request = {
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

    print(
        json.dumps(
            lambda_handler(upsert_s3_assets_and_lineage_request, None),
            indent=4,
        )
    )
