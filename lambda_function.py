import boto3
from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import (
    S3Object,
)

from atlan_operations import *
from constants import logger
from input_validation import validate_input
from s3_operations import get_s3_bucket_objects_and_table_names


def upsert_s3_assets_and_lineage(
    atlan_client,
    aws_session,
    s3_connection_qualified_name,
    asset_owners,
    source_connection_qualified_name,
    source_database_schema_qualified_name,
    target_connection_qualified_name,
    target_database_schema_qualified_name,
    source_extraction_process_name_suffix,
    source_extraction_process_id_suffix,
    target_import_process_name_suffix,
    target_import_process_id_suffix,
    s3_bucket_name,
    s3_bucket_arn,
    qualifier_suffix,
    s3_bucket_prefix=None,
    s3_file_name_pattern=None,
    source_table_pattern=None,
    target_table_pattern=None,
):
    s3_bucket_atlan_arn = f"{s3_bucket_arn}-{qualifier_suffix}"
    bucket_qualified_name = f"{s3_connection_qualified_name}/{s3_bucket_atlan_arn}"

    logger.info("fetching s3 object names")
    s3_objects_and_tablenames = get_s3_bucket_objects_and_table_names(
        aws_session,
        s3_bucket_name,
        s3_prefix=s3_bucket_prefix,
        file_name_regex=s3_file_name_pattern,
    )

    upserted_assets = {
        "s3_bucket_guid": None,
        "s3_objects_guids": [],
        "processes_guids": [],
    }

    logger.info("creating or updating the s3 bucket asset")
    upserted_assets["s3_bucket_guid"] = create_or_update_atlan_s3_bucket(
        atlan_client=atlan_client,
        connection_qualified_name=s3_connection_qualified_name,
        bucket_qualified_name=bucket_qualified_name,
        bucket_name=s3_bucket_name,
        bucket_aws_arn=s3_bucket_atlan_arn,
        bucket_object_count=len(s3_objects_and_tablenames),
        asset_owners=asset_owners

    )

    logger.info("creating or updating s3 assets and their lineage...")
    for s3_obj_name, table_name in s3_objects_and_tablenames:
        s3_object_qualified_name = f"{bucket_qualified_name}/{s3_obj_name}"
        logger.info(f"creating or updating {s3_obj_name} s3 asset...")
        upserted_assets["s3_objects_guids"].append(
            create_or_update_atlan_s3_object(
                atlan_client=atlan_client,
                connection_qualified_name=s3_connection_qualified_name,
                bucket_qualified_name=bucket_qualified_name,
                s3_object_qualified_name=s3_object_qualified_name,
                s3_object_name=s3_obj_name,
                s3_object_aws_arn=f"{s3_bucket_atlan_arn}/{s3_obj_name}",
                asset_owners=asset_owners
            )
        )
        search_pattern = table_name
        if source_table_pattern and "{table_name}" in source_table_pattern:
            search_pattern = source_table_pattern.format(table_name=table_name)
        source_tables = search_table_in_schema(
            atlan_client, source_database_schema_qualified_name, search_pattern
        )
        if source_tables:
            logger.info(
                f"creating or updating source to staging lineage for {table_name}..."
            )
            upserted_assets["processes_guids"].append(
                create_or_update_lineage(
                    atlan_client=atlan_client,
                    process_name=f"{table_name} {source_extraction_process_name_suffix}",
                    process_id=f"{table_name}_{source_extraction_process_id_suffix}",
                    connection_qualified_name=source_connection_qualified_name,
                    inputs=source_tables,
                    outputs=[
                        S3Object.ref_by_qualified_name(
                            qualified_name=s3_object_qualified_name
                        )
                    ],
                    asset_owners=asset_owners
                )
            )

        search_pattern = table_name
        if target_table_pattern and "{table_name}" in target_table_pattern:
            search_pattern = target_table_pattern.format(table_name=table_name)
        target_tables = search_table_in_schema(
            atlan_client, target_database_schema_qualified_name, search_pattern
        )
        if target_tables:
            logger.info(
                f"creating or updating staging to target lineage for {table_name}..."
            )
            upserted_assets["processes_guids"].append(
                create_or_update_lineage(
                    atlan_client=atlan_client,
                    process_name=f"{table_name} {target_import_process_name_suffix}",
                    process_id=f"{table_name} {target_import_process_id_suffix}",
                    connection_qualified_name=target_connection_qualified_name,
                    inputs=[
                        S3Object.ref_by_qualified_name(
                            qualified_name=s3_object_qualified_name
                        )
                    ],
                    outputs=target_tables,
                    asset_owners=asset_owners
                )
            )

    return upserted_assets


def set_to_list(set_):
    if set_ and isinstance(set_, set):
        return list(set_)
    return None


def lambda_handler(req, context):

    logger.info("validating parameters...")
    input = validate_input(req)
    operation = input.operation
    params = input.params

    logger.info("creating api clients...")
    aws_session = boto3.Session()
    atlan_client = AtlanClient()

    result = {"operation": operation}

    if operation == "upsert_s3_connection":
        s3_connection_guid = create_or_update_atlan_s3_connection(
            atlan_client, params.connection_name, params.connection_qn
        )
        result["s3_connection_guid"] = s3_connection_guid

    if operation == "upsert_s3_assets_and_lineage":
        upserted_assets = upsert_s3_assets_and_lineage(
            atlan_client, aws_session, **params.model_dump()
        )
        result["upserted_assets"] = upserted_assets

    if operation == "get_by_guid":
        asset = retrieve_atlan_asset_by_guid(
            atlan_client, params.guid, params.asset_type
        )
        result["asset_info"] = {
            "guid": asset.guid,
            "qualified_name": asset.qualified_name,
            "owners": set_to_list(asset.owner_users),
            "owner_groups": set_to_list(asset.owner_groups),
            "tags": set_to_list(asset.asset_tags),
            "update_time": asset.update_time,
        }

    if operation == "get_by_qn":
        asset = retrieve_atlan_asset_by_qn(
            atlan_client, params.qualified_name, params.asset_type
        )
        result["asset_info"] = {
            "guid": asset.guid,
            "qualified_name": asset.qualified_name,
            "owners": set_to_list(asset.owner_users),
            "owner_groups": set_to_list(asset.owner_groups),
            "tags": set_to_list(asset.asset_tags),
            "update_time": asset.update_time,
        }

    if operation == "purge":
        purge_atlan_assets(atlan_client, params.assets_guids)
        result["purged_assets"] = [str(guid) for guid in params.assets_guids]

    return {"statusCode": 200, "body": result}
