from fastapi import FastAPI
from api.catalog_api import router as CatalogApiRouter
from catalog import get_catalog
from exception import IcebergHTTPException, iceberg_http_exception_handler


app = FastAPI()
app.include_router(CatalogApiRouter)
app.add_exception_handler(IcebergHTTPException, iceberg_http_exception_handler)

# (TODO): remove this
# recreate the db everytime app restarts
for catalog in get_catalog():
    catalog.destroy_tables()
    catalog.create_tables()
