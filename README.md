# iceberg-rest-catalog
Pythonic Iceberg REST Catalog 

Create a virtualenv
```
python -m venv ./venv                     
source ./venv/bin/activate
```

Install project dependencies
```
pip install -r requirements.txt
```

Run the REST Catalog API server
```
fastapi dev main.py
```

Query HTTP URL in web browser
```
http://127.0.0.1:8000/v1/namespaces
```


Currently the REST catalog saves metadata in the local filesystem (under the `/tmp/warehouse/` directory)
This can be made configurable in the future.