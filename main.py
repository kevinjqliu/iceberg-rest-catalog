from fastapi import FastAPI
import uvicorn
from api.catalog_api import router as CatalogApiRouter
from exception import IcebergHTTPException, iceberg_http_exception_handler


app = FastAPI()
app.include_router(CatalogApiRouter)
app.add_exception_handler(IcebergHTTPException, iceberg_http_exception_handler)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
