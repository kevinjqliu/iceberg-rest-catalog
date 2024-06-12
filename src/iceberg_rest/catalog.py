from pyiceberg.catalog.sql import SqlCatalog


def get_catalog():
    warehouse_path = "/tmp/warehouse"
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            # use local file system for pytest
            # "warehouse": f"file://{warehouse_path}",
            # use s3 for spark test
            "warehouse": "s3://warehouse/rest/",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )
    yield catalog
