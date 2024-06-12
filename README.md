# iceberg-rest-catalog
Pythonic Iceberg REST Catalog

## Getting Started

Build the docker image
```
docker build . --target=prod -t iceberg-rest
```

Run the REST Catalog API server
```
docker run --rm -it -p 8000:8000 iceberg-rest
```

Query HTTP URL in web browser
```
http://127.0.0.1:8000/v1/namespaces
```

Currently the REST catalog saves metadata in the local filesystem (under the `/tmp/warehouse/` directory)
This can be made configurable in the future.