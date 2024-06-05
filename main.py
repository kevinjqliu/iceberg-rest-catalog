from typing import Dict, List, Optional, Union

from fastapi import Body, FastAPI, Header, Path, Query
from pydantic import BaseModel, Field, StrictBool, StrictStr

from pyiceberg.table import TableIdentifier, TableRequirement, TableUpdate
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.table.sorting import SortOrder

app = FastAPI()


class Item(BaseModel):
    name: str
    price: float
    is_offer: Union[bool, None] = None


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


@app.put("/items/{item_id}")
def update_item(item_id: int, item: Item):
    return {"item_name": item.name, "item_id": item_id}


from pyiceberg.catalog.sql import SqlCatalog

warehouse_path = "/tmp/warehouse"
catalog = SqlCatalog(
    "default",
    **{
        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
        "warehouse": f"file://{warehouse_path}",
    },
)
# recreate the db
catalog.destroy_tables()
catalog.create_tables()

# from pyiceberg.catalog.sql import SqlCatalog

# warehouse_path = "/tmp/warehouse"
# catalog = SqlCatalog(
#     "default",
#     **{
#         "uri": "sqlite:///:memory:",
#         # "warehouse": f"file://{warehouse_path}",
#         "echo": True,
#     },
# )
# catalog.create_tables()

# from pyiceberg.catalog.rest import RestCatalog
# catalog = RestCatalog(
#     "default",
#     **{
#         "uri": "http://localhost:8181/"
#     },
# )

# /v1/config
class CatalogConfig(BaseModel):
    """
    Server-provided configuration for the catalog.
    """ # noqa: E501
    overrides: Dict[str, StrictStr] = Field(description="Properties that should be used to override client configuration; applied after defaults and client configuration.")
    defaults: Dict[str, StrictStr] = Field(description="Properties that should be used as default configuration; applied before client configuration.")

@app.get(
    "/v1/config",
    # responses={
    #     200: {"model": CatalogConfig, "description": "Server specified configuration values."},
    #     400: {"model": IcebergErrorResponse, "description": "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware."},
    #     401: {"model": IcebergErrorResponse, "description": "Unauthorized. Authentication is required and has failed or has not yet been provided."},
    #     403: {"model": IcebergErrorResponse, "description": "Forbidden. Authenticated user does not have the necessary permissions."},
    #     419: {"model": IcebergErrorResponse, "description": "Credentials have timed out. If possible, the client should refresh credentials and retry."},
    #     503: {"model": IcebergErrorResponse, "description": "The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry."},
    #     500: {"model": IcebergErrorResponse, "description": "A server-side problem that might not be addressable from the client side. Used for server 500 errors without more specific documentation in individual routes."},
    # },
    tags=["Configuration API"],
    summary="List all catalog configuration settings",
    response_model_by_alias=True,
)
def get_config(
    warehouse: str = Query(None, description="Warehouse location or identifier to request from the service", alias="warehouse"),
    # token_OAuth2: TokenModel = Security(
    #     get_token_OAuth2, scopes=["catalog"]
    # ),
    # token_BearerAuth: TokenModel = Security(
    #     get_token_BearerAuth
    # ),
) -> CatalogConfig:
    """ All REST clients should first call this route to get catalog configuration properties from the server to configure the catalog and its HTTP client. Configuration from the server consists of two sets of key/value pairs. - defaults -  properties that should be used as default configuration; applied before client configuration - overrides - properties that should be used to override client configuration; applied after defaults and client configuration  Catalog configuration is constructed by setting the defaults, then client- provided configuration, and finally overrides. The final property set is then used to configure the catalog.  For example, a default configuration property might set the size of the client pool, which can be replaced with a client-specific setting. An override might be used to set the warehouse location, which is stored on the server rather than in client configuration.  Common catalog configuration settings are documented at https://iceberg.apache.org/docs/latest/configuration/#catalog-properties """
    return CatalogConfig(overrides={}, defaults={})


# /v1/{prefix}/namespaces (GET/POST)
class CreateNamespaceRequest(BaseModel):
    """
    CreateNamespaceRequest
    """ # noqa: E501
    namespace: List[StrictStr] = Field(description="Reference to one or more levels of a namespace")
    properties: Optional[Dict[str, StrictStr]] = Field(default=None, description="Configured string to string map of properties for the namespace")

class CreateNamespaceResponse(BaseModel):
    """
    CreateNamespaceResponse
    """ # noqa: E501
    namespace: List[StrictStr] = Field(description="Reference to one or more levels of a namespace")
    properties: Optional[Dict[str, StrictStr]] = Field(default=None, description="Properties stored on the namespace, if supported by the server.")


@app.post(
    "/v1/namespaces",
    # responses={
    #     200: {"model": CreateNamespaceResponse, "description": "Represents a successful call to create a namespace. Returns the namespace created, as well as any properties that were stored for the namespace, including those the server might have added. Implementations are not required to support namespace properties."},
    #     # 400: {"model": IcebergErrorResponse, "description": "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware."},
    #     # 401: {"model": IcebergErrorResponse, "description": "Unauthorized. Authentication is required and has failed or has not yet been provided."},
    #     # 403: {"model": IcebergErrorResponse, "description": "Forbidden. Authenticated user does not have the necessary permissions."},
    #     # 406: {"model": ErrorModel, "description": "Not Acceptable / Unsupported Operation. The server does not support this operation."},
    #     # 409: {"model": IcebergErrorResponse, "description": "Conflict - The namespace already exists"},
    #     # 419: {"model": IcebergErrorResponse, "description": "Credentials have timed out. If possible, the client should refresh credentials and retry."},
    #     # 503: {"model": IcebergErrorResponse, "description": "The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry."},
    #     # 500: {"model": IcebergErrorResponse, "description": "A server-side problem that might not be addressable from the client side. Used for server 500 errors without more specific documentation in individual routes."},
    # },
    tags=["Catalog API"],
    summary="Create a namespace",
    response_model_by_alias=True,
)
def create_namespace(
    # prefix: str = Path(..., description="An optional prefix in the path"),
    create_namespace_request: CreateNamespaceRequest = Body(None, description=""),
    # token_OAuth2: TokenModel = Security(
    #     get_token_OAuth2, scopes=["catalog"]
    # ),
    # token_BearerAuth: TokenModel = Security(
    #     get_token_BearerAuth
    # ),
) -> CreateNamespaceResponse:
    """Create a namespace, with an optional set of properties. The server might also add properties, such as &#x60;last_modified_time&#x60; etc."""
    namespace = "".join(create_namespace_request.namespace)
    properties = create_namespace_request.properties
    catalog.create_namespace(namespace, properties)
    return CreateNamespaceResponse(namespace=create_namespace_request.namespace, properties=create_namespace_request.properties)

class ListNamespacesResponse(BaseModel):
    """
    ListNamespacesResponse
    """ # noqa: E501
    # next_page_token: Optional[StrictStr] = Field(default=None, description="An opaque token which allows clients to make use of pagination for a list API (e.g. ListTables). Clients will initiate the first paginated request by sending an empty `pageToken` e.g. `GET /tables?pageToken` or `GET /tables?pageToken=` signaling to the service that the response should be paginated. Servers that support pagination will recognize `pageToken` and return a `next-page-token` in response if there are more results available. After the initial request, it is expected that the value of `next-page-token` from the last response is used in the subsequent request. Servers that do not support pagination will ignore `next-page-token` and return all results.", alias="next-page-token")
    namespaces: Optional[List[List[StrictStr]]] = None


@app.get(
    "/v1/namespaces",
    # responses={
    #     200: {"model": ListNamespacesResponse, "description": "A list of namespaces"},
    #     # 400: {"model": IcebergErrorResponse, "description": "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware."},
    #     # 401: {"model": IcebergErrorResponse, "description": "Unauthorized. Authentication is required and has failed or has not yet been provided."},
    #     # 403: {"model": IcebergErrorResponse, "description": "Forbidden. Authenticated user does not have the necessary permissions."},
    #     # 404: {"model": IcebergErrorResponse, "description": "Not Found - Namespace provided in the &#x60;parent&#x60; query parameter is not found."},
    #     # 419: {"model": IcebergErrorResponse, "description": "Credentials have timed out. If possible, the client should refresh credentials and retry."},
    #     # 503: {"model": IcebergErrorResponse, "description": "The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry."},
    #     # 500: {"model": IcebergErrorResponse, "description": "A server-side problem that might not be addressable from the client side. Used for server 500 errors without more specific documentation in individual routes."},
    # },
    tags=["Catalog API"],
    summary="List namespaces, optionally providing a parent namespace to list underneath",
    response_model_by_alias=True,
)
def list_namespaces(
    # prefix: str = Path(..., description="An optional prefix in the path"),
    # page_token: str = Query(None, description="", alias="pageToken"),
    # page_size: int = Query(None, description="For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated &#x60;pageSize&#x60;.", alias="pageSize", ge=1),
    # parent: str = Query(None, description="An optional namespace, underneath which to list namespaces. If not provided or empty, all top-level namespaces should be listed. If parent is a multipart namespace, the parts must be separated by the unit separator (&#x60;0x1F&#x60;) byte.", alias="parent"),
    # token_OAuth2: TokenModel = Security(
    #     get_token_OAuth2, scopes=["catalog"]
    # ),
    # token_BearerAuth: TokenModel = Security(
    #     get_token_BearerAuth
    # ),
) -> ListNamespacesResponse:
    """List all namespaces at a certain level, optionally starting from a given parent namespace. If table accounting.tax.paid.info exists, using &#39;SELECT NAMESPACE IN accounting&#39; would translate into &#x60;GET /namespaces?parent&#x3D;accounting&#x60; and must return a namespace, [\&quot;accounting\&quot;, \&quot;tax\&quot;] only. Using &#39;SELECT NAMESPACE IN accounting.tax&#39; would translate into &#x60;GET /namespaces?parent&#x3D;accounting%1Ftax&#x60; and must return a namespace, [\&quot;accounting\&quot;, \&quot;tax\&quot;, \&quot;paid\&quot;]. If &#x60;parent&#x60; is not provided, all top-level namespaces should be listed."""
    return ListNamespacesResponse(namespaces=catalog.list_namespaces())

# /v1/{prefix}/namespaces/{namespace} (GET/DELETE/HEAD)
class GetNamespaceResponse(BaseModel):
    """
    GetNamespaceResponse
    """ # noqa: E501
    namespace: List[StrictStr] = Field(description="Reference to one or more levels of a namespace")
    properties: Optional[Dict[str, StrictStr]] = Field(default=None, description="Properties stored on the namespace, if supported by the server. If the server does not support namespace properties, it should return null for this field. If namespace properties are supported, but none are set, it should return an empty object.")

@app.get(
    "/v1/namespaces/{namespace}",
    # responses={
    #     200: {"model": GetNamespaceResponse, "description": "Returns a namespace, as well as any properties stored on the namespace if namespace properties are supported by the server."},
    #     400: {"model": IcebergErrorResponse, "description": "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware."},
    #     401: {"model": IcebergErrorResponse, "description": "Unauthorized. Authentication is required and has failed or has not yet been provided."},
    #     403: {"model": IcebergErrorResponse, "description": "Forbidden. Authenticated user does not have the necessary permissions."},
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - Namespace not found"},
    #     419: {"model": IcebergErrorResponse, "description": "Credentials have timed out. If possible, the client should refresh credentials and retry."},
    #     503: {"model": IcebergErrorResponse, "description": "The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry."},
    #     500: {"model": IcebergErrorResponse, "description": "A server-side problem that might not be addressable from the client side. Used for server 500 errors without more specific documentation in individual routes."},
    # },
    tags=["Catalog API"],
    summary="Load the metadata properties for a namespace",
    response_model_by_alias=True,
)
def load_namespace_metadata(
    # prefix: str = Path(..., description="An optional prefix in the path"),
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
    # token_OAuth2: TokenModel = Security(
    #     get_token_OAuth2, scopes=["catalog"]
    # ),
    # token_BearerAuth: TokenModel = Security(
    #     get_token_BearerAuth
    # ),
) -> GetNamespaceResponse:
    """Return all stored metadata properties for a given namespace"""
    properties = catalog.load_namespace_properties(namespace=namespace)
    return GetNamespaceResponse(namespace=[namespace], properties=properties)

@app.delete(
    "/v1/namespaces/{namespace}",
    # responses={
    #     204: {"description": "Success, no content"},
    #     400: {"model": IcebergErrorResponse, "description": "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware."},
    #     401: {"model": IcebergErrorResponse, "description": "Unauthorized. Authentication is required and has failed or has not yet been provided."},
    #     403: {"model": IcebergErrorResponse, "description": "Forbidden. Authenticated user does not have the necessary permissions."},
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - Namespace to delete does not exist."},
    #     419: {"model": IcebergErrorResponse, "description": "Credentials have timed out. If possible, the client should refresh credentials and retry."},
    #     503: {"model": IcebergErrorResponse, "description": "The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry."},
    #     500: {"model": IcebergErrorResponse, "description": "A server-side problem that might not be addressable from the client side. Used for server 500 errors without more specific documentation in individual routes."},
    # },
    tags=["Catalog API"],
    summary="Drop a namespace from the catalog. Namespace must be empty.",
    response_model_by_alias=True,
)
def drop_namespace(
    # prefix: str = Path(..., description="An optional prefix in the path"),
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
    # token_OAuth2: TokenModel = Security(
    #     get_token_OAuth2, scopes=["catalog"]
    # ),
    # token_BearerAuth: TokenModel = Security(
    #     get_token_BearerAuth
    # ),
) -> None:
    catalog.drop_namespace(namespace)


# @app.head(
#     "/v1/namespaces/{namespace}",
#     # responses={
#     #     204: {"description": "Success, no content"},
#     #     400: {"model": IcebergErrorResponse, "description": "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware."},
#     #     401: {"model": IcebergErrorResponse, "description": "Unauthorized. Authentication is required and has failed or has not yet been provided."},
#     #     403: {"model": IcebergErrorResponse, "description": "Forbidden. Authenticated user does not have the necessary permissions."},
#     #     404: {"model": IcebergErrorResponse, "description": "Not Found - Namespace not found"},
#     #     419: {"model": IcebergErrorResponse, "description": "Credentials have timed out. If possible, the client should refresh credentials and retry."},
#     #     503: {"model": IcebergErrorResponse, "description": "The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry."},
#     #     500: {"model": IcebergErrorResponse, "description": "A server-side problem that might not be addressable from the client side. Used for server 500 errors without more specific documentation in individual routes."},
#     # },
#     tags=["Catalog API"],
#     summary="Check if a namespace exists",
#     response_model_by_alias=True,
# )
# def namespace_exists(
#     # prefix: str = Path(..., description="An optional prefix in the path"),
#     namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
#     # token_OAuth2: TokenModel = Security(
#     #     get_token_OAuth2, scopes=["catalog"]
#     # ),
#     # token_BearerAuth: TokenModel = Security(
#     #     get_token_BearerAuth
#     # ),
# ) -> None:
#     """Check if a namespace exists. The response does not contain a body."""
#     return BaseCatalogAPIApi.subclasses[0]().namespace_exists(prefix, namespace)

# /v1/{prefix}/namespaces/{namespace}/properties (POST)
class UpdateNamespacePropertiesRequest(BaseModel):
    """
    UpdateNamespacePropertiesRequest
    """ # noqa: E501
    removals: Optional[List[StrictStr]] = None
    updates: Optional[Dict[str, StrictStr]] = None

class UpdateNamespacePropertiesResponse(BaseModel):
    """
    UpdateNamespacePropertiesResponse
    """ # noqa: E501
    updated: List[StrictStr] = Field(description="List of property keys that were added or updated")
    removed: List[StrictStr] = Field(description="List of properties that were removed")
    missing: Optional[List[StrictStr]] = Field(default=None, description="List of properties requested for removal that were not found in the namespace's properties. Represents a partial success response. Server's do not need to implement this.")

@app.post(
    "/v1/namespaces/{namespace}/properties",
    # responses={
    #     200: {"model": UpdateNamespacePropertiesResponse, "description": "JSON data response for a synchronous update properties request."},
    #     400: {"model": IcebergErrorResponse, "description": "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware."},
    #     401: {"model": IcebergErrorResponse, "description": "Unauthorized. Authentication is required and has failed or has not yet been provided."},
    #     403: {"model": IcebergErrorResponse, "description": "Forbidden. Authenticated user does not have the necessary permissions."},
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - Namespace not found"},
    #     406: {"model": ErrorModel, "description": "Not Acceptable / Unsupported Operation. The server does not support this operation."},
    #     422: {"model": IcebergErrorResponse, "description": "Unprocessable Entity - A property key was included in both &#x60;removals&#x60; and &#x60;updates&#x60;"},
    #     419: {"model": IcebergErrorResponse, "description": "Credentials have timed out. If possible, the client should refresh credentials and retry."},
    #     503: {"model": IcebergErrorResponse, "description": "The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry."},
    #     500: {"model": IcebergErrorResponse, "description": "A server-side problem that might not be addressable from the client side. Used for server 500 errors without more specific documentation in individual routes."},
    # },
    tags=["Catalog API"],
    summary="Set or remove properties on a namespace",
    response_model_by_alias=True,
)
def update_properties(
    # prefix: str = Path(..., description="An optional prefix in the path"),
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
    update_namespace_properties_request: UpdateNamespacePropertiesRequest = Body(None, description=""),
    # token_OAuth2: TokenModel = Security(
    #     get_token_OAuth2, scopes=["catalog"]
    # ),
    # token_BearerAuth: TokenModel = Security(
    #     get_token_BearerAuth
    # ),
) -> UpdateNamespacePropertiesResponse:
    """Set and/or remove properties on a namespace. The request body specifies a list of properties to remove and a map of key value pairs to update. Properties that are not in the request are not modified or removed by this call. Server implementations are not required to support namespace properties."""
    properties_update_summary = catalog.update_namespace_properties(namespace=namespace, removals=set(update_namespace_properties_request.removals), updates=update_namespace_properties_request.updates)
    return UpdateNamespacePropertiesResponse(updated=properties_update_summary.updated, removed=properties_update_summary.removed, missing=properties_update_summary.missing)

# /v1/{prefix}/namespaces/{namespace}/tables (GET/POST)
class ListTablesResponse(BaseModel):
    """
    ListTablesResponse
    """ # noqa: E501
    next_page_token: Optional[StrictStr] = Field(default=None, description="An opaque token which allows clients to make use of pagination for a list API (e.g. ListTables). Clients will initiate the first paginated request by sending an empty `pageToken` e.g. `GET /tables?pageToken` or `GET /tables?pageToken=` signaling to the service that the response should be paginated. Servers that support pagination will recognize `pageToken` and return a `next-page-token` in response if there are more results available. After the initial request, it is expected that the value of `next-page-token` from the last response is used in the subsequent request. Servers that do not support pagination will ignore `next-page-token` and return all results.", alias="next-page-token")
    identifiers: Optional[List[TableIdentifier]] = None

@app.get(
    "/v1/namespaces/{namespace}/tables",
    # responses={
    #     200: {"model": ListTablesResponse, "description": "A list of table identifiers"},
    #     400: {"model": IcebergErrorResponse, "description": "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware."},
    #     401: {"model": IcebergErrorResponse, "description": "Unauthorized. Authentication is required and has failed or has not yet been provided."},
    #     403: {"model": IcebergErrorResponse, "description": "Forbidden. Authenticated user does not have the necessary permissions."},
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - The namespace specified does not exist"},
    #     419: {"model": IcebergErrorResponse, "description": "Credentials have timed out. If possible, the client should refresh credentials and retry."},
    #     503: {"model": IcebergErrorResponse, "description": "The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry."},
    #     500: {"model": IcebergErrorResponse, "description": "A server-side problem that might not be addressable from the client side. Used for server 500 errors without more specific documentation in individual routes."},
    # },
    tags=["Catalog API"],
    summary="List all table identifiers underneath a given namespace",
    response_model_by_alias=True,
)
def list_tables(
    # prefix: str = Path(..., description="An optional prefix in the path"),
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
    page_token: str = Query(None, description="", alias="pageToken"),
    page_size: int = Query(None, description="For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated &#x60;pageSize&#x60;.", alias="pageSize", ge=1),
    # token_OAuth2: TokenModel = Security(
    #     get_token_OAuth2, scopes=["catalog"]
    # ),
    # token_BearerAuth: TokenModel = Security(
    #     get_token_BearerAuth
    # ),
) -> ListTablesResponse:
    """Return all table identifiers under this namespace"""
    identifiers = catalog.list_tables(namespace=namespace)
    table_identifiers = [TableIdentifier(namespace=[identifier[0]], name=identifier[1]) for identifier in identifiers] 
    return ListTablesResponse(identifiers=table_identifiers)

class CreateTableRequest(BaseModel):
    """
    CreateTableRequest
    """ # noqa: E501
    name: StrictStr
    location: Optional[StrictStr] = None
    schema: Schema = Field(alias="schema")
    partition_spec: Optional[PartitionSpec] = Field(default=None, alias="partition-spec")
    write_order: Optional[SortOrder] = Field(default=None, alias="write-order")
    stage_create: Optional[StrictBool] = Field(default=None, alias="stage-create")
    properties: Optional[Dict[str, StrictStr]] = None

class LoadTableResult(BaseModel):
    """
    Result used when a table is successfully loaded.   The table metadata JSON is returned in the `metadata` field. The corresponding file location of table metadata should be returned in the `metadata-location` field, unless the metadata is not yet committed. For example, a create transaction may return metadata that is staged but not committed. Clients can check whether metadata has changed by comparing metadata locations after the table has been created.   The `config` map returns table-specific configuration for the table's resources, including its HTTP client and FileIO. For example, config may contain a specific FileIO implementation class for the table depending on its underlying storage.   The following configurations should be respected by clients:  ## General Configurations  - `token`: Authorization bearer token to use for table requests if OAuth2 security is enabled   ## AWS Configurations  The following configurations should be respected when working with tables stored in AWS S3  - `client.region`: region to configure client for making requests to AWS  - `s3.access-key-id`: id for for credentials that provide access to the data in S3  - `s3.secret-access-key`: secret for credentials that provide access to data in S3   - `s3.session-token`: if present, this value should be used for as the session token   - `s3.remote-signing-enabled`: if `true` remote signing should be performed as described in the `s3-signer-open-api.yaml` specification 
    """ # noqa: E501
    metadata_location: Optional[StrictStr] = Field(default=None, description="May be null if the table is staged as part of a transaction")#, alias="metadata-location")
    metadata: TableMetadata
    config: Optional[Dict[str, StrictStr]] = None


@app.post(
    "/v1/namespaces/{namespace}/tables",
    # responses={
    #     200: {"model": LoadTableResult, "description": "Table metadata result after creating a table"},
    #     400: {"model": IcebergErrorResponse, "description": "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware."},
    #     401: {"model": IcebergErrorResponse, "description": "Unauthorized. Authentication is required and has failed or has not yet been provided."},
    #     403: {"model": IcebergErrorResponse, "description": "Forbidden. Authenticated user does not have the necessary permissions."},
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - The namespace specified does not exist"},
    #     409: {"model": IcebergErrorResponse, "description": "Conflict - The table already exists"},
    #     419: {"model": IcebergErrorResponse, "description": "Credentials have timed out. If possible, the client should refresh credentials and retry."},
    #     503: {"model": IcebergErrorResponse, "description": "The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry."},
    #     500: {"model": IcebergErrorResponse, "description": "A server-side problem that might not be addressable from the client side. Used for server 500 errors without more specific documentation in individual routes."},
    # },
    tags=["Catalog API"],
    summary="Create a table in the given namespace",
    response_model_by_alias=True,
)
def create_table(
    # prefix: str = Path(..., description="An optional prefix in the path"),
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
    # x_iceberg_access_delegation: str = Header(None, description="Optional signal to the server that the client supports delegated access via a comma-separated list of access mechanisms.  The server may choose to supply access via any or none of the requested mechanisms.  Specific properties and handling for &#x60;vended-credentials&#x60; is documented in the &#x60;LoadTableResult&#x60; schema section of this spec document.  The protocol and specification for &#x60;remote-signing&#x60; is documented in  the &#x60;s3-signer-open-api.yaml&#x60; OpenApi spec in the &#x60;aws&#x60; module. "),
    create_table_request: CreateTableRequest = Body(None, description=""),
    # token_OAuth2: TokenModel = Security(
    #     get_token_OAuth2, scopes=["catalog"]
    # ),
    # token_BearerAuth: TokenModel = Security(
    #     get_token_BearerAuth
    # ),
) -> LoadTableResult:
        # identifier: Union[str, Identifier],
        # schema: Union[Schema, "pa.Schema"],
        # location: Optional[str] = None,
        # partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        # sort_order: SortOrder = UNSORTED_SORT_ORDER,
        # properties: Properties = EMPTY_DICT,

        # name: StrictStr
        # location: Optional[StrictStr] = None
        # schema: Schema = Field(alias="schema")
        # partition_spec: Optional[PartitionSpec] = Field(default=None, alias="partition-spec")
        # write_order: Optional[SortOrder] = Field(default=None, alias="write-order")
        # stage_create: Optional[StrictBool] = Field(default=None, alias="stage-create")
        # properties: Optional[Dict[str, StrictStr]] = None

    """Create a table or start a create transaction, like atomic CTAS.  If &#x60;stage-create&#x60; is false, the table is created immediately.  If &#x60;stage-create&#x60; is true, the table is not created, but table metadata is initialized and returned. The service should prepare as needed for a commit to the table commit endpoint to complete the create transaction. The client uses the returned metadata to begin a transaction. To commit the transaction, the client sends all create and subsequent changes to the table commit route. Changes from the table create operation include changes like AddSchemaUpdate and SetCurrentSchemaUpdate that set the initial table state."""
    tbl = catalog.create_table(
        identifier=(namespace, create_table_request.name), 
        schema=create_table_request.schema, 
        location=create_table_request.location,
        partition_spec=create_table_request.partition_spec, 
        sort_order=create_table_request.write_order,
        properties=create_table_request.properties
    )
    return LoadTableResult(metadata_location=tbl.metadata_location, metadata=tbl.metadata, config=tbl.properties)

# /v1/{prefix}/namespaces/{namespace}/register (POST)
class RegisterTableRequest(BaseModel):
    """
    RegisterTableRequest
    """ # noqa: E501
    name: StrictStr
    metadata_location: StrictStr = Field(alias="metadata-location")

@app.post(
    "/v1/namespaces/{namespace}/register",
    # responses={
    #     200: {"model": LoadTableResult, "description": "Table metadata result when loading a table"},
    #     400: {"model": IcebergErrorResponse, "description": "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware."},
    #     401: {"model": IcebergErrorResponse, "description": "Unauthorized. Authentication is required and has failed or has not yet been provided."},
    #     403: {"model": IcebergErrorResponse, "description": "Forbidden. Authenticated user does not have the necessary permissions."},
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - The namespace specified does not exist"},
    #     409: {"model": IcebergErrorResponse, "description": "Conflict - The table already exists"},
    #     419: {"model": IcebergErrorResponse, "description": "Credentials have timed out. If possible, the client should refresh credentials and retry."},
    #     503: {"model": IcebergErrorResponse, "description": "The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry."},
    #     500: {"model": IcebergErrorResponse, "description": "A server-side problem that might not be addressable from the client side. Used for server 500 errors without more specific documentation in individual routes."},
    # },
    tags=["Catalog API"],
    summary="Register a table in the given namespace using given metadata file location",
    response_model_by_alias=True,
)
def register_table(
    # prefix: str = Path(..., description="An optional prefix in the path"),
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
    register_table_request: RegisterTableRequest = Body(None, description=""),
    # token_OAuth2: TokenModel = Security(
    #     get_token_OAuth2, scopes=["catalog"]
    # ),
    # token_BearerAuth: TokenModel = Security(
    #     get_token_BearerAuth
    # ),
) -> LoadTableResult:
    """Register a table using given metadata file location."""
    tbl = catalog.register_table(identifier=(namespace, register_table_request.name), metadata_location=register_table_request.metadata_location)
    return LoadTableResult(metadata_location=tbl.metadata_location, metadata=tbl.metadata, config=tbl.properties)

# /v1/{prefix}/namespaces/{namespace}/tables/{table} (GET/POST/DELETE/HEAD)
@app.get(
    "/v1/namespaces/{namespace}/tables/{table}",
    # responses={
    #     200: {"model": LoadTableResult, "description": "Table metadata result when loading a table"},
    #     400: {"model": IcebergErrorResponse, "description": "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware."},
    #     401: {"model": IcebergErrorResponse, "description": "Unauthorized. Authentication is required and has failed or has not yet been provided."},
    #     403: {"model": IcebergErrorResponse, "description": "Forbidden. Authenticated user does not have the necessary permissions."},
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - NoSuchTableException, table to load does not exist"},
    #     419: {"model": IcebergErrorResponse, "description": "Credentials have timed out. If possible, the client should refresh credentials and retry."},
    #     503: {"model": IcebergErrorResponse, "description": "The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry."},
    #     500: {"model": IcebergErrorResponse, "description": "A server-side problem that might not be addressable from the client side. Used for server 500 errors without more specific documentation in individual routes."},
    # },
    tags=["Catalog API"],
    summary="Load a table from the catalog",
    response_model_by_alias=True,
)
def load_table(
    # prefix: str = Path(..., description="An optional prefix in the path"),
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
    table: str = Path(..., description="A table name"),
    x_iceberg_access_delegation: str = Header(None, description="Optional signal to the server that the client supports delegated access via a comma-separated list of access mechanisms.  The server may choose to supply access via any or none of the requested mechanisms.  Specific properties and handling for &#x60;vended-credentials&#x60; is documented in the &#x60;LoadTableResult&#x60; schema section of this spec document.  The protocol and specification for &#x60;remote-signing&#x60; is documented in  the &#x60;s3-signer-open-api.yaml&#x60; OpenApi spec in the &#x60;aws&#x60; module. "),
    snapshots: str = Query(None, description="The snapshots to return in the body of the metadata. Setting the value to &#x60;all&#x60; would return the full set of snapshots currently valid for the table. Setting the value to &#x60;refs&#x60; would load all snapshots referenced by branches or tags. Default if no param is provided is &#x60;all&#x60;.", alias="snapshots"),
    # token_OAuth2: TokenModel = Security(
    #     get_token_OAuth2, scopes=["catalog"]
    # ),
    # token_BearerAuth: TokenModel = Security(
    #     get_token_BearerAuth
    # ),
) -> LoadTableResult:
    """Load a table from the catalog.  The response contains both configuration and table metadata. The configuration, if non-empty is used as additional configuration for the table that overrides catalog configuration. For example, this configuration may change the FileIO implementation to be used for the table.  The response also contains the table&#39;s full metadata, matching the table metadata JSON file.  The catalog configuration may contain credentials that should be used for subsequent requests for the table. The configuration key \&quot;token\&quot; is used to pass an access token to be used as a bearer token for table requests. Otherwise, a token may be passed using a RFC 8693 token type as a configuration key. For example, \&quot;urn:ietf:params:oauth:token-type:jwt&#x3D;&lt;JWT-token&gt;\&quot;."""
    tbl = catalog.load_table(identifier=(namespace, table))
    return LoadTableResult(metadata_location=tbl.metadata_location, metadata=tbl.metadata, config=tbl.properties)

class CommitTableRequest(BaseModel):
    """
    CommitTableRequest
    """ # noqa: E501
    identifier: Optional[TableIdentifier] = None
    requirements: List[TableRequirement]
    updates: List[TableUpdate]

class CommitTableResponse(BaseModel):
    """
    CommitTableResponse
    """ # noqa: E501
    metadata_location: StrictStr = Field(alias="metadata-location")
    metadata: TableMetadata

@app.post(
    "/v1/namespaces/{namespace}/tables/{table}",
    # responses={
    #     200: {"model": CommitTableResponse, "description": "Response used when a table is successfully updated. The table metadata JSON is returned in the metadata field. The corresponding file location of table metadata must be returned in the metadata-location field. Clients can check whether metadata has changed by comparing metadata locations."},
    #     400: {"model": IcebergErrorResponse, "description": "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware."},
    #     401: {"model": IcebergErrorResponse, "description": "Unauthorized. Authentication is required and has failed or has not yet been provided."},
    #     403: {"model": IcebergErrorResponse, "description": "Forbidden. Authenticated user does not have the necessary permissions."},
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - NoSuchTableException, table to load does not exist"},
    #     409: {"model": IcebergErrorResponse, "description": "Conflict - CommitFailedException, one or more requirements failed. The client may retry."},
    #     419: {"model": IcebergErrorResponse, "description": "Credentials have timed out. If possible, the client should refresh credentials and retry."},
    #     500: {"model": IcebergErrorResponse, "description": "An unknown server-side problem occurred; the commit state is unknown."},
    #     503: {"model": IcebergErrorResponse, "description": "The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry."},
    #     502: {"model": IcebergErrorResponse, "description": "A gateway or proxy received an invalid response from the upstream server; the commit state is unknown."},
    #     504: {"model": IcebergErrorResponse, "description": "A server-side gateway timeout occurred; the commit state is unknown."},
    #     500: {"model": IcebergErrorResponse, "description": "A server-side problem that might not be addressable on the client."},
    # },
    tags=["Catalog API"],
    summary="Commit updates to a table",
    response_model_by_alias=True,
)
def update_table(
    # prefix: str = Path(..., description="An optional prefix in the path"),
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
    table: str = Path(..., description="A table name"),
    commit_table_request: CommitTableRequest = Body(None, description=""),
    # token_OAuth2: TokenModel = Security(
    #     get_token_OAuth2, scopes=["catalog"]
    # ),
    # token_BearerAuth: TokenModel = Security(
    #     get_token_BearerAuth
    # ),
) -> CommitTableResponse:
    """Commit updates to a table.  Commits have two parts, requirements and updates. Requirements are assertions that will be validated before attempting to make and commit changes. For example, &#x60;assert-ref-snapshot-id&#x60; will check that a named ref&#39;s snapshot ID has a certain value.  Updates are changes to make to table metadata. For example, after asserting that the current main ref is at the expected snapshot, a commit may add a new child snapshot and set the ref to the new snapshot id.  Create table transactions that are started by createTable with &#x60;stage-create&#x60; set to true are committed using this route. Transactions should include all changes to the table, including table initialization, like AddSchemaUpdate and SetCurrentSchemaUpdate. The &#x60;assert-create&#x60; requirement is used to ensure that the table was not created concurrently."""
    return catalog._commit_table(commit_table_request)

@app.delete(
    "/v1/namespaces/{namespace}/tables/{table}",
    # responses={
    #     204: {"description": "Success, no content"},
    #     400: {"model": IcebergErrorResponse, "description": "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware."},
    #     401: {"model": IcebergErrorResponse, "description": "Unauthorized. Authentication is required and has failed or has not yet been provided."},
    #     403: {"model": IcebergErrorResponse, "description": "Forbidden. Authenticated user does not have the necessary permissions."},
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - NoSuchTableException, Table to drop does not exist"},
    #     419: {"model": IcebergErrorResponse, "description": "Credentials have timed out. If possible, the client should refresh credentials and retry."},
    #     503: {"model": IcebergErrorResponse, "description": "The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry."},
    #     500: {"model": IcebergErrorResponse, "description": "A server-side problem that might not be addressable from the client side. Used for server 500 errors without more specific documentation in individual routes."},
    # },
    tags=["Catalog API"],
    summary="Drop a table from the catalog",
    response_model_by_alias=True,
)
def drop_table(
    # prefix: str = Path(..., description="An optional prefix in the path"),
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
    table: str = Path(..., description="A table name"),
    purge_requested: bool = Query(False, description="Whether the user requested to purge the underlying table&#39;s data and metadata", alias="purgeRequested"),
    # token_OAuth2: TokenModel = Security(
    #     get_token_OAuth2, scopes=["catalog"]
    # ),
    # token_BearerAuth: TokenModel = Security(
    #     get_token_BearerAuth
    # ),
) -> None:
    """Remove a table from the catalog"""
    return catalog.drop_table(identifier=(namespace, table))


@app.head(
    "/v1/namespaces/{namespace}/tables/{table}",
    # responses={
    #     204: {"description": "Success, no content"},
    #     400: {"model": IcebergErrorResponse, "description": "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware."},
    #     401: {"model": IcebergErrorResponse, "description": "Unauthorized. Authentication is required and has failed or has not yet been provided."},
    #     403: {"model": IcebergErrorResponse, "description": "Forbidden. Authenticated user does not have the necessary permissions."},
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - NoSuchTableException, Table not found"},
    #     419: {"model": IcebergErrorResponse, "description": "Credentials have timed out. If possible, the client should refresh credentials and retry."},
    #     503: {"model": IcebergErrorResponse, "description": "The service is not ready to handle the request. The client should wait and retry.  The service may additionally send a Retry-After header to indicate when to retry."},
    #     500: {"model": IcebergErrorResponse, "description": "A server-side problem that might not be addressable from the client side. Used for server 500 errors without more specific documentation in individual routes."},
    # },
    tags=["Catalog API"],
    summary="Check if a table exists",
    response_model_by_alias=True,
)
def table_exists(
    # prefix: str = Path(..., description="An optional prefix in the path"),
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
    table: str = Path(..., description="A table name"),
    # token_OAuth2: TokenModel = Security(
    #     get_token_OAuth2, scopes=["catalog"]
    # ),
    # token_BearerAuth: TokenModel = Security(
    #     get_token_BearerAuth
    # ),
) -> None:
    """Check if a table exists within a given namespace. The response does not contain a body."""
    catalog.load_table(identifier=(namespace, table))

# /v1/{prefix}/transactions/commit (POST)


# /v1/oauth/tokens
# /v1/{prefix}/namespaces/{namespace}/views
# /v1/{prefix}/namespaces/{namespace}/views/{view}
# /v1/{prefix}/views/rename
# /v1/{prefix}/tables/rename (POST)
# /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics (POST)
