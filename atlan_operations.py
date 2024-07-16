from pyatlan.cache.role_cache import RoleCache
from pyatlan.client.atlan import AtlanClient
from pyatlan.errors import NotFoundError
from pyatlan.model.assets import (
    Connection,
    S3Bucket,
    S3Object,
    Process,
    Table,
)
from pyatlan.model.enums import AtlanConnectorType
from pyatlan.model.fluent_search import FluentSearch, CompoundQuery

from constants import ASSET_TYPES


def create_or_update_atlan_s3_connection(atlan_client: AtlanClient, name, qualified_name):
    admin_role_guid = RoleCache.get_id_for_name("$admin")
    s3_connection = Connection.creator(
        name=name,
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
    asset_owners=None
):
    s3bucket = S3Bucket.creator(
        name=bucket_name,
        connection_qualified_name=connection_qualified_name,
        aws_arn=bucket_aws_arn,
    )
    s3bucket.qualified_name = bucket_qualified_name
    s3bucket.s3_object_count = bucket_object_count
    s3bucket.owner_users = asset_owners
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
    asset_owners=None
):
    s3_object = S3Object.creator(
        name=s3_object_name,
        connection_qualified_name=connection_qualified_name,
        aws_arn=s3_object_aws_arn,
        s3_bucket_qualified_name=bucket_qualified_name,
    )
    s3_object.qualified_name = s3_object_qualified_name
    s3_object.owner_users = asset_owners
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
            asset_type=ASSET_TYPES[asset_type], guid=str(guid)
        )
    except NotFoundError:
        return None


def purge_atlan_assets(atlan_client, assets_guids):
    for asset_guid in assets_guids:
        atlan_client.asset.purge_by_guid(str(asset_guid))


def create_or_update_lineage(
    atlan_client, process_name, process_id, connection_qualified_name, inputs, outputs, asset_owners=[]
):    
    atlan_process = Process.creator(
        name=process_name,
        connection_qualified_name=connection_qualified_name,
        process_id=process_id,
        inputs=inputs,
        outputs=outputs,
    )
    atlan_process.owner_users = asset_owners
    response = atlan_client.asset.save(atlan_process)
    process_guid = list(response.guid_assignments.values())[0]
    return process_guid


def search_table_in_schema(atlan_client, schema_qn, table_name_regex):
    request = (
        FluentSearch()
        .where(CompoundQuery.asset_type(Table))
        .where(CompoundQuery.active_assets())
        .where(Table.QUALIFIED_NAME.startswith(schema_qn))
        .where(Table.NAME.regexp(table_name_regex, case_insensitive=True))
    ).to_request()

    tables = []
    for result in atlan_client.asset.search(request):
        if isinstance(result, Table):
            tables.append(result)
    return tables
