from typing import Dict, Optional

from fastapi import Body, FastAPI, HTTPException, Header, Path, Query
from pydantic import BaseModel, Field, StrictStr

from pyiceberg.table import TableIdentifier
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.exceptions import TableAlreadyExistsError, NoSuchTableError, NamespaceAlreadyExistsError, NoSuchNamespaceError, NamespaceNotEmptyError

from models.config import CatalogConfig
from models.request import CommitTableRequest, CommitTransactionRequest, CreateNamespaceRequest, CreateTableRequest, RegisterTableRequest, RenameTableRequest, UpdateNamespacePropertiesRequest
from models.response import CommitTableResponse, CreateNamespaceResponse, GetNamespaceResponse, ListNamespacesResponse, ListTablesResponse, UpdateNamespacePropertiesResponse

app = FastAPI()

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

@app.get("/reset")
def reset():
    catalog.destroy_tables()
    catalog.create_tables()
    return {"status": "ok"}


# /v1/config
@app.get(
    "/v1/config",
    # responses={
    # },
    tags=["Configuration API"],
    summary="List all catalog configuration settings",
    response_model_by_alias=True,
)
def get_config(
    warehouse: str = Query(None, description="Warehouse location or identifier to request from the service", alias="warehouse"),
) -> CatalogConfig:
    """ All REST clients should first call this route to get catalog configuration properties from the server to configure the catalog and its HTTP client. Configuration from the server consists of two sets of key/value pairs. - defaults -  properties that should be used as default configuration; applied before client configuration - overrides - properties that should be used to override client configuration; applied after defaults and client configuration  Catalog configuration is constructed by setting the defaults, then client- provided configuration, and finally overrides. The final property set is then used to configure the catalog.  For example, a default configuration property might set the size of the client pool, which can be replaced with a client-specific setting. An override might be used to set the warehouse location, which is stored on the server rather than in client configuration.  Common catalog configuration settings are documented at https://iceberg.apache.org/docs/latest/configuration/#catalog-properties """
    return CatalogConfig(overrides={}, defaults={})


# /v1/{prefix}/namespaces (GET/POST)
@app.post(
    "/v1/namespaces",
    # responses={
    #     # 400: {"model": IcebergErrorResponse, "description": "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware."},
    #     # 406: {"model": ErrorModel, "description": "Not Acceptable / Unsupported Operation. The server does not support this operation."},
    #     # 409: {"model": IcebergErrorResponse, "description": "Conflict - The namespace already exists"},
    # },
    tags=["Catalog API"],
    summary="Create a namespace",
    response_model_by_alias=True,
)
def create_namespace(
    create_namespace_request: CreateNamespaceRequest = Body(None, description=""),
) -> CreateNamespaceResponse:
    """Create a namespace, with an optional set of properties. The server might also add properties, such as &#x60;last_modified_time&#x60; etc."""
    namespace = tuple(create_namespace_request.namespace)
    properties = create_namespace_request.properties
    try:
        catalog.create_namespace(namespace, properties)
    except NamespaceAlreadyExistsError:
        raise HTTPException(status_code=409, detail=f"Namespace already exists: {namespace}")
    return CreateNamespaceResponse(namespace=namespace, properties=properties)

@app.get(
    "/v1/namespaces",
    # responses={
    #     # 400: {"model": IcebergErrorResponse, "description": "Indicates a bad request error. It could be caused by an unexpected request body format or other forms of request validation failure, such as invalid json. Usually serves application/json content, although in some cases simple text/plain content might be returned by the server&#39;s middleware."},
    #     # 404: {"model": IcebergErrorResponse, "description": "Not Found - Namespace provided in the &#x60;parent&#x60; query parameter is not found."},
    # },
    tags=["Catalog API"],
    summary="List namespaces, optionally providing a parent namespace to list underneath",
    response_model_by_alias=True,
)
def list_namespaces(
    # page_token: str = Query(None, description="", alias="pageToken"),
    # page_size: int = Query(None, description="For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated &#x60;pageSize&#x60;.", alias="pageSize", ge=1),
    # parent: str = Query(None, description="An optional namespace, underneath which to list namespaces. If not provided or empty, all top-level namespaces should be listed. If parent is a multipart namespace, the parts must be separated by the unit separator (&#x60;0x1F&#x60;) byte.", alias="parent"),
) -> ListNamespacesResponse:
    """List all namespaces at a certain level, optionally starting from a given parent namespace. If table accounting.tax.paid.info exists, using &#39;SELECT NAMESPACE IN accounting&#39; would translate into &#x60;GET /namespaces?parent&#x3D;accounting&#x60; and must return a namespace, [\&quot;accounting\&quot;, \&quot;tax\&quot;] only. Using &#39;SELECT NAMESPACE IN accounting.tax&#39; would translate into &#x60;GET /namespaces?parent&#x3D;accounting%1Ftax&#x60; and must return a namespace, [\&quot;accounting\&quot;, \&quot;tax\&quot;, \&quot;paid\&quot;]. If &#x60;parent&#x60; is not provided, all top-level namespaces should be listed."""
    return ListNamespacesResponse(namespaces=catalog.list_namespaces())

# /v1/{prefix}/namespaces/{namespace} (GET/DELETE/HEAD)
@app.get(
    "/v1/namespaces/{namespace}",
    # responses={
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - Namespace not found"},
    # },
    tags=["Catalog API"],
    summary="Load the metadata properties for a namespace",
    response_model_by_alias=True,
)
def load_namespace_metadata(
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
) -> GetNamespaceResponse:
    """Return all stored metadata properties for a given namespace"""
    try:
        properties = catalog.load_namespace_properties(namespace=namespace)
    except NoSuchNamespaceError:
        raise HTTPException(status_code=404, detail=f"Namespace does not exist: {namespace}")
    return GetNamespaceResponse(namespace=[namespace], properties=properties)

@app.delete(
    "/v1/namespaces/{namespace}",
    # responses={
    #     204: {"description": "Success, no content"},
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - Namespace to delete does not exist."},
    # },
    tags=["Catalog API"],
    summary="Drop a namespace from the catalog. Namespace must be empty.",
    response_model_by_alias=True,
)
def drop_namespace(
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
) -> None:
    try:
        catalog.drop_namespace(namespace)
    except NoSuchNamespaceError:
        raise HTTPException(status_code=404, detail=f"Namespace does not exist: {(namespace,)}")
    except NamespaceNotEmptyError:
        raise HTTPException(status_code=409, detail=f"Namespace is not empty: {(namespace,)}")


@app.head(
    "/v1/namespaces/{namespace}",
    # responses={
    #     204: {"description": "Success, no content"},
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - Namespace not found"},
    # },
    tags=["Catalog API"],
    summary="Check if a namespace exists",
    response_model_by_alias=True,
)
def namespace_exists(
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
) -> None:
    """Check if a namespace exists. The response does not contain a body."""
    try:
        catalog.load_namespace_properties(namespace=namespace)
    except NoSuchNamespaceError:
        raise HTTPException(status_code=404, detail=f"Namespace does not exist: {namespace}")

# /v1/{prefix}/namespaces/{namespace}/properties (POST)
@app.post(
    "/v1/namespaces/{namespace}/properties",
    # responses={
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - Namespace not found"},
    #     406: {"model": ErrorModel, "description": "Not Acceptable / Unsupported Operation. The server does not support this operation."},
    #     422: {"model": IcebergErrorResponse, "description": "Unprocessable Entity - A property key was included in both &#x60;removals&#x60; and &#x60;updates&#x60;"},
    # },
    tags=["Catalog API"],
    summary="Set or remove properties on a namespace",
    response_model_by_alias=True,
)
def update_namespace_properties(
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
    update_namespace_properties_request: UpdateNamespacePropertiesRequest = Body(None, description=""),
) -> UpdateNamespacePropertiesResponse:
    """Set and/or remove properties on a namespace. The request body specifies a list of properties to remove and a map of key value pairs to update. Properties that are not in the request are not modified or removed by this call. Server implementations are not required to support namespace properties."""
    try:
        summary = catalog.update_namespace_properties(namespace=namespace, removals=set(update_namespace_properties_request.removals), updates=update_namespace_properties_request.updates)
    except NoSuchNamespaceError:
        raise HTTPException(status_code=404, detail=f"Namespace does not exist: {namespace}")
    return UpdateNamespacePropertiesResponse(updated=sorted(summary.updated), removed=sorted(summary.removed), missing=sorted(summary.missing))

# /v1/{prefix}/namespaces/{namespace}/tables (GET/POST)
@app.get(
    "/v1/namespaces/{namespace}/tables",
    # responses={
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - The namespace specified does not exist"},
    # },
    tags=["Catalog API"],
    summary="List all table identifiers underneath a given namespace",
    response_model_by_alias=True,
)
def list_tables(
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
    page_token: str = Query(None, description="", alias="pageToken"),
    page_size: int = Query(None, description="For servers that support pagination, this signals an upper bound of the number of results that a client will receive. For servers that do not support pagination, clients may receive results larger than the indicated &#x60;pageSize&#x60;.", alias="pageSize", ge=1),
) -> ListTablesResponse:
    """Return all table identifiers under this namespace"""
    identifiers = catalog.list_tables(namespace=namespace)
    table_identifiers = [TableIdentifier(namespace=[identifier[0]], name=identifier[1]) for identifier in identifiers] 
    return ListTablesResponse(identifiers=table_identifiers)


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
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - The namespace specified does not exist"},
    #     409: {"model": IcebergErrorResponse, "description": "Conflict - The table already exists"},
    # },
    tags=["Catalog API"],
    summary="Create a table in the given namespace",
    response_model_by_alias=True,
)
def create_table(
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
    # x_iceberg_access_delegation: str = Header(None, description="Optional signal to the server that the client supports delegated access via a comma-separated list of access mechanisms.  The server may choose to supply access via any or none of the requested mechanisms.  Specific properties and handling for &#x60;vended-credentials&#x60; is documented in the &#x60;LoadTableResult&#x60; schema section of this spec document.  The protocol and specification for &#x60;remote-signing&#x60; is documented in  the &#x60;s3-signer-open-api.yaml&#x60; OpenApi spec in the &#x60;aws&#x60; module. "),
    create_table_request: CreateTableRequest = Body(None, description=""),
) -> LoadTableResult:
    """Create a table or start a create transaction, like atomic CTAS.  If &#x60;stage-create&#x60; is false, the table is created immediately.  If &#x60;stage-create&#x60; is true, the table is not created, but table metadata is initialized and returned. The service should prepare as needed for a commit to the table commit endpoint to complete the create transaction. The client uses the returned metadata to begin a transaction. To commit the transaction, the client sends all create and subsequent changes to the table commit route. Changes from the table create operation include changes like AddSchemaUpdate and SetCurrentSchemaUpdate that set the initial table state."""
    try:
        identifier = (namespace, create_table_request.name)
        tbl = catalog.create_table(
            identifier=identifier, 
            schema=create_table_request.schema, 
            location=create_table_request.location,
            partition_spec=create_table_request.partition_spec, 
            sort_order=create_table_request.write_order,
            properties=create_table_request.properties
        )
    except TableAlreadyExistsError:
        raise HTTPException(status_code=409, detail=f"Table already exists: {identifier}")
    return LoadTableResult(metadata_location=tbl.metadata_location, metadata=tbl.metadata, config=tbl.properties)

# /v1/{prefix}/namespaces/{namespace}/register (POST)
@app.post(
    "/v1/namespaces/{namespace}/register",
    # responses={
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - The namespace specified does not exist"},
    #     409: {"model": IcebergErrorResponse, "description": "Conflict - The table already exists"},
    # },
    tags=["Catalog API"],
    summary="Register a table in the given namespace using given metadata file location",
    response_model_by_alias=True,
)
def register_table(
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
    register_table_request: RegisterTableRequest = Body(None, description=""),
) -> LoadTableResult:
    """Register a table using given metadata file location."""
    tbl = catalog.register_table(identifier=(namespace, register_table_request.name), metadata_location=register_table_request.metadata_location)
    return LoadTableResult(metadata_location=tbl.metadata_location, metadata=tbl.metadata, config=tbl.properties)

# /v1/{prefix}/namespaces/{namespace}/tables/{table} (GET/POST/DELETE/HEAD)
@app.get(
    "/v1/namespaces/{namespace}/tables/{table}",
    # responses={
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - NoSuchTableException, table to load does not exist"},
    # },
    tags=["Catalog API"],
    summary="Load a table from the catalog",
    response_model_by_alias=True,
)
def load_table(
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
    table: str = Path(..., description="A table name"),
    x_iceberg_access_delegation: str = Header(None, description="Optional signal to the server that the client supports delegated access via a comma-separated list of access mechanisms.  The server may choose to supply access via any or none of the requested mechanisms.  Specific properties and handling for &#x60;vended-credentials&#x60; is documented in the &#x60;LoadTableResult&#x60; schema section of this spec document.  The protocol and specification for &#x60;remote-signing&#x60; is documented in  the &#x60;s3-signer-open-api.yaml&#x60; OpenApi spec in the &#x60;aws&#x60; module. "),
    snapshots: str = Query(None, description="The snapshots to return in the body of the metadata. Setting the value to &#x60;all&#x60; would return the full set of snapshots currently valid for the table. Setting the value to &#x60;refs&#x60; would load all snapshots referenced by branches or tags. Default if no param is provided is &#x60;all&#x60;.", alias="snapshots"),
) -> LoadTableResult:
    """Load a table from the catalog.  The response contains both configuration and table metadata. The configuration, if non-empty is used as additional configuration for the table that overrides catalog configuration. For example, this configuration may change the FileIO implementation to be used for the table.  The response also contains the table&#39;s full metadata, matching the table metadata JSON file.  The catalog configuration may contain credentials that should be used for subsequent requests for the table. The configuration key \&quot;token\&quot; is used to pass an access token to be used as a bearer token for table requests. Otherwise, a token may be passed using a RFC 8693 token type as a configuration key. For example, \&quot;urn:ietf:params:oauth:token-type:jwt&#x3D;&lt;JWT-token&gt;\&quot;."""
    try:
        identifier = (namespace, table)
        tbl = catalog.load_table(identifier=identifier)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table does not exist: {identifier}")
    return LoadTableResult(metadata_location=tbl.metadata_location, metadata=tbl.metadata, config=tbl.properties)


@app.post(
    "/v1/namespaces/{namespace}/tables/{table}",
    # responses={
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - NoSuchTableException, table to load does not exist"},
    #     409: {"model": IcebergErrorResponse, "description": "Conflict - CommitFailedException, one or more requirements failed. The client may retry."},
    #     500: {"model": IcebergErrorResponse, "description": "An unknown server-side problem occurred; the commit state is unknown."},
    #     502: {"model": IcebergErrorResponse, "description": "A gateway or proxy received an invalid response from the upstream server; the commit state is unknown."},
    #     504: {"model": IcebergErrorResponse, "description": "A server-side gateway timeout occurred; the commit state is unknown."},
    #     500: {"model": IcebergErrorResponse, "description": "A server-side problem that might not be addressable on the client."},
    # },
    tags=["Catalog API"],
    summary="Commit updates to a table",
    response_model_by_alias=True,
)
def update_table(
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
    table: str = Path(..., description="A table name"),
    commit_table_request: CommitTableRequest = Body(None, description=""),
) -> CommitTableResponse:
    """Commit updates to a table.  Commits have two parts, requirements and updates. Requirements are assertions that will be validated before attempting to make and commit changes. For example, &#x60;assert-ref-snapshot-id&#x60; will check that a named ref&#39;s snapshot ID has a certain value.  Updates are changes to make to table metadata. For example, after asserting that the current main ref is at the expected snapshot, a commit may add a new child snapshot and set the ref to the new snapshot id.  Create table transactions that are started by createTable with &#x60;stage-create&#x60; set to true are committed using this route. Transactions should include all changes to the table, including table initialization, like AddSchemaUpdate and SetCurrentSchemaUpdate. The &#x60;assert-create&#x60; requirement is used to ensure that the table was not created concurrently."""
    return catalog._commit_table(commit_table_request)

@app.delete(
    "/v1/namespaces/{namespace}/tables/{table}",
    # responses={
    #     204: {"description": "Success, no content"},
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - NoSuchTableException, Table to drop does not exist"},
    # },
    tags=["Catalog API"],
    summary="Drop a table from the catalog",
    response_model_by_alias=True,
)
def drop_table(
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
    table: str = Path(..., description="A table name"),
    purge_requested: bool = Query(False, description="Whether the user requested to purge the underlying table&#39;s data and metadata", alias="purgeRequested"),
) -> None:
    """Remove a table from the catalog"""
    return catalog.drop_table(identifier=(namespace, table))


@app.head(
    "/v1/namespaces/{namespace}/tables/{table}",
    # responses={
    #     204: {"description": "Success, no content"},
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - NoSuchTableException, Table not found"},
    # },
    tags=["Catalog API"],
    summary="Check if a table exists",
    response_model_by_alias=True,
)
def table_exists(
    namespace: str = Path(..., description="A namespace identifier as a single string. Multipart namespace parts should be separated by the unit separator (&#x60;0x1F&#x60;) byte."),
    table: str = Path(..., description="A table name"),
) -> None:
    """Check if a table exists within a given namespace. The response does not contain a body."""
    catalog.load_table(identifier=(namespace, table))

# /v1/{prefix}/transactions/commit (POST)
@app.post(
    "/v1/{prefix}/transactions/commit",
    # responses={
    #     204: {"description": "Success, no content"},
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - NoSuchTableException, table to load does not exist"},
    #     409: {"model": IcebergErrorResponse, "description": "Conflict - CommitFailedException, one or more requirements failed. The client may retry."},
    #     500: {"model": IcebergErrorResponse, "description": "An unknown server-side problem occurred; the commit state is unknown."},
    #     502: {"model": IcebergErrorResponse, "description": "A gateway or proxy received an invalid response from the upstream server; the commit state is unknown."},
    #     504: {"model": IcebergErrorResponse, "description": "A server-side gateway timeout occurred; the commit state is unknown."},
    #     500: {"model": IcebergErrorResponse, "description": "A server-side problem that might not be addressable on the client."},
    # },
    tags=["Catalog API"],
    summary="Commit updates to multiple tables in an atomic operation",
    response_model_by_alias=True,
)
def commit_transaction(
    commit_transaction_request: CommitTransactionRequest = Body(None, description="Commit updates to multiple tables in an atomic operation  A commit for a single table consists of a table identifier with requirements and updates. Requirements are assertions that will be validated before attempting to make and commit changes. For example, &#x60;assert-ref-snapshot-id&#x60; will check that a named ref&#39;s snapshot ID has a certain value.  Updates are changes to make to table metadata. For example, after asserting that the current main ref is at the expected snapshot, a commit may add a new child snapshot and set the ref to the new snapshot id."),
) -> None:
    ...

# /v1/{prefix}/tables/rename (POST)
@app.post(
    "/v1/tables/rename",
    # responses={
    #     204: {"description": "Success, no content"},
    #     404: {"model": IcebergErrorResponse, "description": "Not Found - NoSuchTableException, Table to rename does not exist - NoSuchNamespaceException, The target namespace of the new table identifier does not exist"},
    #     406: {"model": ErrorModel, "description": "Not Acceptable / Unsupported Operation. The server does not support this operation."},
    #     409: {"model": IcebergErrorResponse, "description": "Conflict - The target identifier to rename to already exists as a table or view"},
    # },
    tags=["Catalog API"],
    summary="Rename a table from its current name to a new name",
    response_model_by_alias=True,
)
def rename_table(
    rename_table_request: RenameTableRequest = Body(None, description="Current table identifier to rename and new table identifier to rename to"),
) -> None:
    """Rename a table from one identifier to another. It&#39;s valid to move a table across namespaces, but the server implementation is not required to support it."""
    source = (".".join(rename_table_request.source.namespace.root), rename_table_request.source.name)
    destination = (".".join(rename_table_request.destination.namespace.root), rename_table_request.destination.name)
    try:
        catalog.rename_table(source, destination)
    except NoSuchNamespaceError:
        raise HTTPException(status_code=404, detail=f"Namespace does not exist: {source}")
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table does not exist: {source}")
    except TableAlreadyExistsError:
        raise HTTPException(status_code=409, detail=f"Table already exists: {destination}")


# /v1/oauth/tokens
# /v1/{prefix}/namespaces/{namespace}/views
# /v1/{prefix}/namespaces/{namespace}/views/{view}
# /v1/{prefix}/views/rename
# /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics (POST)
