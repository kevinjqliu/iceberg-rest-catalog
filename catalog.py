from config import CatalogSettings
from pyiceberg.catalog.sql import SqlCatalog

catalog_settings = CatalogSettings()


class Catalog:
    instance = None

    def __new__(cls):
        if cls.instance is None:
            cls.instance = _create_catalog()
        return cls.instance


def _create_catalog():
    catalog = SqlCatalog(
        catalog_settings.catalog_name, **catalog_settings.catalog_config
    )
    # (TODO): remove this
    # recreate the db everytime app restarts
    catalog.destroy_tables()
    catalog.create_tables()
    return catalog


def get_catalog() -> Catalog:
    return Catalog()
