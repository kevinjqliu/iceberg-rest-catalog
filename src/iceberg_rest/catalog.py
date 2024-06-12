from iceberg_rest.settings import settings
from pyiceberg.catalog.sql import SqlCatalog


def get_catalog():
    catalog = SqlCatalog(
        settings.CATALOG_NAME,
        **{
            "uri": settings.CATALOG_URI,
            "warehouse": settings.CATALOG_WAREHOUSE,
            "s3.endpoint": settings.CATALOG_S3_ENDPOINT,
            "s3.access-key-id": settings.AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": settings.AWS_SECRET_ACCESS_KEY,
        },
    )
    yield catalog
