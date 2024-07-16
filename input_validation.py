from pydantic import BaseModel, constr, conlist, field_validator
from typing import Optional, Union
from uuid import UUID

from constants import ASSET_TYPES


class UpsertS3ConnectionParams(BaseModel):
    connection_qn: constr(min_length=1)  # type:ignore
    connection_name: constr(min_length=1)  # type:ignore
    asset_owners: Optional[list[str]]


class UpsertS3ConnectionRequest(BaseModel):
    operation: constr(pattern="^upsert_s3_connection$")  # type:ignore
    params: UpsertS3ConnectionParams


class GetByGuidParams(BaseModel):
    guid: UUID
    asset_type: constr(min_length=1)  # type:ignore

    @field_validator("asset_type")
    @classmethod
    def check_asset_type(cls, asset_type):
        if asset_type not in ASSET_TYPES:
            raise ValueError("asset_type must be a valid atlan asset type")
        return asset_type


class GetByGuidRequest(BaseModel):
    operation: constr(pattern="^get_by_guid$")  # type:ignore
    params: GetByGuidParams


class GetByQnParams(BaseModel):
    qualified_name: constr(min_length=1)  # type:ignore
    asset_type: constr(min_length=1)  # type:ignore

    @field_validator("asset_type")
    @classmethod
    def check_asset_type(cls, asset_type):
        if asset_type not in ASSET_TYPES:
            raise ValueError("asset_type must be a valid atlan asset type")
        return asset_type


class GetByQnRequest(BaseModel):
    operation: constr(pattern="^get_by_qn$")  # type:ignore
    params: GetByQnParams


class PurgeParams(BaseModel):
    assets_guids: conlist(UUID, min_length=1)  # type:ignore


class PurgeRequest(BaseModel):
    operation: constr(pattern="^purge$")  # type:ignore
    params: PurgeParams


class UpsertS3AssetsAndLineageParams(BaseModel):
    s3_connection_qualified_name: constr(min_length=1)  # type:ignore
    asset_owners: Optional[list[str]]
    source_connection_qualified_name: constr(min_length=1)  # type:ignore
    source_database_schema_qualified_name: constr(min_length=1)  # type:ignore
    target_connection_qualified_name: constr(min_length=1)  # type:ignore
    target_database_schema_qualified_name: constr(min_length=1)  # type:ignore
    source_extraction_process_name_suffix: constr(min_length=1)  # type:ignore
    source_extraction_process_id_suffix: constr(min_length=1)  # type:ignore
    target_import_process_name_suffix: constr(min_length=1)  # type:ignore
    target_import_process_id_suffix: constr(min_length=1)  # type:ignore
    s3_bucket_name: constr(min_length=1)  # type:ignore
    s3_bucket_arn: constr(min_length=1)  # type:ignore
    qualifier_suffix: constr(min_length=1)  # type:ignore
    s3_bucket_prefix: Optional[str]
    s3_file_name_pattern: Optional[str]
    source_table_pattern: Optional[str]
    target_table_pattern: Optional[str]


class UpsertS3AssetsAndLineageRequest(BaseModel):
    operation: constr(pattern="^upsert_s3_assets_and_lineage$")  # type:ignore
    params: UpsertS3AssetsAndLineageParams


class RequestSchema(BaseModel):
    request: Union[
        UpsertS3ConnectionRequest,
        GetByGuidRequest,
        GetByQnRequest,
        PurgeRequest,
        UpsertS3AssetsAndLineageRequest,
    ]


def validate_input(req):
    request = {"request": req}
    return RequestSchema(**request).request


if __name__ == "__main__":
    upsert_s3_connection_request = {
        "operation": "upsert_s3_connection",
        "params": {
            "connection_qn": "default/s3/1720796029",
        },
    }

    result = validate_input(upsert_s3_connection_request)
