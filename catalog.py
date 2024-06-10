from pyiceberg.catalog.sql import SqlCatalog


class Catalog:
    instance = None

    def __new__(cls):
        if cls.instance is None:
            cls.instance = _create_catalog()
        return cls.instance


def _create_catalog():
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
    return catalog


def get_catalog() -> Catalog:
    catalog = Catalog()
    # (TODO): remove this
    # recreate the db everytime app restarts
    catalog.destroy_tables()
    catalog.create_tables()
    return catalog
