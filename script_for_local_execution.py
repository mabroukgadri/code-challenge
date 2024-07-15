import json
import logging

import boto3
from pyatlan.cache.role_cache import RoleCache
from pyatlan.client.atlan import AtlanClient
from pyatlan.errors import NotFoundError
from pyatlan.model.assets import (
    Connection,
    S3Bucket,
    S3Object,
    Process,
    Table,
    Database,
    Schema,
)
from pyatlan.model.enums import AtlanConnectorType

logger = logging.getLogger()
if len(logging.getLogger().handlers) > 0:
    # if code is executed within a lambda
    logger.setLevel(logging.INFO)
else:
    # if code is executed locally
    logging.basicConfig(level=logging.INFO)

ASSET_TYPES = {
    "Connection": Connection,
    "S3Bucket": S3Bucket,
    "S3Object": S3Object,
    "Process": Process,
    "Database": Database,
    "Schema": Schema,
    "Table": Table,
}

S3_CONNECTION_NAME = "aws-s3-connection-mag"
ASSETS_OWNER = "mag.i"



def create_or_update_atlan_s3_connection(atlan_client: AtlanClient, qualified_name):
    admin_role_guid = RoleCache.get_id_for_name("$admin")
    s3_connection = Connection.creator(
        name=S3_CONNECTION_NAME,
        connector_type=AtlanConnectorType.S3,
        admin_roles=[admin_role_guid],
    )
    s3_connection.qualified_name = qualified_name
    response = atlan_client.asset.save(s3_connection)
    connection_guid = list(response.guid_assignments.values())[0]
    return connection_guid


def create_or_update_atlan_s3_bucket(
    atlan_client,
    connection_qualified_name,
    bucket_qualified_name,
    bucket_name,
    bucket_aws_arn,
    bucket_object_count,
):
    s3bucket = S3Bucket.creator(
        name=bucket_name,
        connection_qualified_name=connection_qualified_name,
        aws_arn=bucket_aws_arn,
    )
    s3bucket.qualified_name = bucket_qualified_name
    s3bucket.s3_object_count = bucket_object_count
    s3bucket.owner_users = [ASSETS_OWNER]
    response = atlan_client.asset.save(s3bucket)
    bucket_guid = list(response.guid_assignments.values())[0]
    return bucket_guid


def create_or_update_atlan_s3_object(
    atlan_client,
    connection_qualified_name,
    bucket_qualified_name,
    s3_object_qualified_name,
    s3_object_name,
    s3_object_aws_arn,
):
    s3_object = S3Object.creator(
        name=s3_object_name,
        connection_qualified_name=connection_qualified_name,
        aws_arn=s3_object_aws_arn,
        s3_bucket_qualified_name=bucket_qualified_name,
    )
    s3_object.qualified_name = s3_object_qualified_name
    s3_object.owner_users = [ASSETS_OWNER]
    response = atlan_client.asset.save(s3_object)
    object_guid = list(response.guid_assignments.values())[0]
    return object_guid


def retrieve_atlan_asset_by_qn(atlan_client, qualified_name, asset_type):
    try:
        return atlan_client.asset.get_by_qualified_name(
            asset_type=ASSET_TYPES[asset_type], qualified_name=qualified_name
        )
    except NotFoundError:
        return None


def retrieve_atlan_asset_by_guid(atlan_client, guid, asset_type):
    try:
        return atlan_client.asset.get_by_guid(
            asset_type=ASSET_TYPES[asset_type], guid=guid
        )
    except NotFoundError:
        return None


def purge_atlan_assets(atlan_client, assets_guids):
    for asset_guid in assets_guids:
        atlan_client.asset.purge_by_guid(asset_guid)


def create_or_update_lineage(
    atlan_client, process_name, process_id, connection_qualified_name, inputs, outputs
):
    atlan_process = Process.creator(
        name=process_name,
        connection_qualified_name=connection_qualified_name,
        process_id=process_id,
        inputs=inputs,
        outputs=outputs,
    )
    atlan_process.owner_users = [ASSETS_OWNER]
    response = atlan_client.asset.save(atlan_process)
    process_guid = list(response.guid_assignments.values())[0]
    return process_guid


def list_s3_bucket_objects(aws_session, bucket_name, s3_prefix=""):

    s3 = aws_session.resource("s3")
    bucket = s3.Bucket(bucket_name)

    s3_object_list = []
    for s3_object in bucket.objects.filter(Prefix=s3_prefix):
        s3_object_list.append(s3_object.key)
    return s3_object_list


def upsert_s3_assets_and_lineage(
    atlan_client,
    s3_connection_qualified_name,
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
):

    s3_bucket_atlan_arn = f"{s3_bucket_arn}-{qualifier_suffix}"
    bucket_qualified_name = f"{s3_connection_qualified_name}/{s3_bucket_atlan_arn}"

    logger.info("fetching s3 object names")
    aws_session = boto3.Session()
    s3_object_list = list_s3_bucket_objects(aws_session, s3_bucket_name)

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
        bucket_object_count=len(s3_object_list),
    )

    logger.info("creating or updating s3 assets and their lineage...")
    for s3_obj_name in s3_object_list:
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
            )
        )

        table_name = s3_obj_name.split(".")[0]

        source_table = retrieve_atlan_asset_by_qn(
            atlan_client,
            f"{source_database_schema_qualified_name}/{table_name}",
            "Table",
        )
        if source_table is not None:
            logger.info(f"creating or updating source to staging lineage for {table_name}...")
            upserted_assets["processes_guids"].append(
                create_or_update_lineage(
                    atlan_client=atlan_client,
                    process_name=f"{table_name} {source_extraction_process_name_suffix}",
                    process_id=f"{table_name}_{source_extraction_process_id_suffix}",
                    connection_qualified_name=source_connection_qualified_name,
                    inputs=[source_table],
                    outputs=[
                        S3Object.ref_by_qualified_name(
                            qualified_name=s3_object_qualified_name
                        )
                    ],
                )
            )

        target_table = retrieve_atlan_asset_by_qn(
            atlan_client,
            f"{target_database_schema_qualified_name}/{table_name}",
            "Table",
        )
        if target_table is not None:
            logger.info(f"creating or updating staging to target lineage for {table_name}...")
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
                    outputs=[target_table],
                )
            )

    return upserted_assets


class WrongInputException(Exception):
    pass


def validate_parameters(req):
    operation = req.get("operation")
    params = req.get("params", {})

    if not (operation and params):
        raise WrongInputException(Exception)
    
    if operation == "upsert_s3_connection":
        connection_qn = params.get("connection_qn")
        if not connection_qn or not isinstance(connection_qn, str):
            raise WrongInputException(Exception)
        return ("upsert_s3_connection", connection_qn)

    if operation == "get_by_guid":
        guid = params.get("guid")
        asset_type = params.get("asset_type")
        if not guid or not isinstance(guid, str) or asset_type not in ASSET_TYPES:
            raise WrongInputException(Exception)
        return ("get_by_guid", (guid, asset_type))

    elif operation == "get_by_qn":
        qualified_name = params.get("qualified_name")
        asset_type = params.get("asset_type")
        if (
            not qualified_name
            or not isinstance(qualified_name, str)
            or asset_type not in ASSET_TYPES
        ):
            raise WrongInputException(Exception)
        return ("get_by_qn", (qualified_name, asset_type))

    elif operation == "purge":
        assets_guids = params.get("assets_guids")
        if not assets_guids or not isinstance(assets_guids, list):
            raise WrongInputException(Exception)
        return ("purge", assets_guids)

    elif operation == "upsert_s3_assets_and_lineage":
        s3_connection_qualified_name = params.get("s3_connection_qualified_name")
        source_connection_qualified_name = params.get(
            "source_connection_qualified_name"
        )
        source_database_schema_qualified_name = params.get(
            "source_database_schema_qualified_name"
        )
        target_connection_qualified_name = params.get(
            "target_connection_qualified_name"
        )
        target_database_schema_qualified_name = params.get(
            "target_database_schema_qualified_name"
        )
        source_extraction_process_name_suffix = params.get(
            "source_extraction_process_name_suffix"
        )
        source_extraction_process_id_suffix = params.get(
            "source_extraction_process_id_suffix"
        )
        target_import_process_name_suffix = params.get(
            "target_import_process_name_suffix"
        )
        target_import_process_id_suffix = params.get("target_import_process_id_suffix")
        s3_bucket_name = params.get("s3_bucket_name")
        s3_bucket_arn = params.get("s3_bucket_arn")
        qualifier_suffix = params.get("qualifier_suffix")

        if not (
            s3_connection_qualified_name
            and source_connection_qualified_name
            and source_database_schema_qualified_name
            and target_connection_qualified_name
            and target_database_schema_qualified_name
            and source_extraction_process_name_suffix
            and source_extraction_process_id_suffix
            and target_import_process_name_suffix
            and target_import_process_id_suffix
            and s3_bucket_name
            and s3_bucket_arn
            and qualifier_suffix
        ):
            raise WrongInputException("wrong payload schema")
        else:
            return (
                "upsert_s3_assets_and_lineage",
                (
                    s3_connection_qualified_name,
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
                ),
            )
    else:
        raise WrongInputException()


def list_or_none(set_):
    if set_ and isinstance(set_, set):
        return list(set_)
    return None


def lambda_handler(req, context):

    logger.info("validating parameters...")
    operation, params = validate_parameters(req)

    logger.info("creating atlan api client...")
    atlan_client = AtlanClient()

    result = {"operation": operation}

    if operation == "upsert_s3_connection":
        qualified_name = params
        s3_connection_guid = create_or_update_atlan_s3_connection(atlan_client, qualified_name)
        result["s3_connection_guid"] = s3_connection_guid
        return result

    if operation == "upsert_s3_assets_and_lineage":
        upserted_assets = upsert_s3_assets_and_lineage(atlan_client, *params)
        result["upserted_assets"] = upserted_assets
        return result

    if operation == "get_by_guid":
        guid, asset_type = params
        asset = retrieve_atlan_asset_by_guid(atlan_client, guid, asset_type)
        result["asset_info"] = {
            "guid": asset.guid,
            "qualified_name": asset.qualified_name,
            "owners": list_or_none(asset.owner_users),
            "owner_groups": list_or_none(asset.owner_groups),
            "tags": list_or_none(asset.asset_tags),
            "update_time": asset.update_time,
        }
        return result

    if operation == "get_by_qn":
        qualified_name, asset_type = params
        asset = retrieve_atlan_asset_by_qn(atlan_client, qualified_name, asset_type)
        result["asset_info"] = {
            "guid": asset.guid,
            "qualified_name": asset.qualified_name,
            "owners": list_or_none(asset.owner_users),
            "owner_groups": list_or_none(asset.owner_groups),
            "tags": list_or_none(asset.asset_tags),
            "update_time": asset.update_time,
        }
        return result

    if operation == "purge":
        assets_guids = params
        purge_atlan_assets(atlan_client, assets_guids)
        result["purged_assets"] = assets_guids
        return result


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
