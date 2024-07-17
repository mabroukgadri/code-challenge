import logging

from pyatlan.model.assets import (
    Connection,
    S3Bucket,
    S3Object,
    Process,
    Table,
    Database,
    Schema,
)


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
