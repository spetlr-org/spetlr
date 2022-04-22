
# DeltaHandle and DbHandle

The `TableConfigurator` contains logic to distinguish between production and 
debug tables. To make full use of this functionality when reading and writing 
delta tables, two convenience classes, `DeltaHandle` and `DbHandle`, have 
been provided. Use the classes like this

```python
from atc.config_master import TableConfigurator
from atc.delta import DeltaHandle, DbHandle

tc = TableConfigurator()
tc.add_resource_path('/my/config/files')

# name is mandatory,
# path is optional
# format is optional. Must equal 'db' if provided
db = DbHandle.from_tc('MyDb')

# quickly create the database
db.create()

# name is mandatory,
# path is optional
# format is optional. Must equal 'delta' if provided
dh = DeltaHandle.from_tc('MyTblId')

# quickly create table without schema
dh.create_hive_table()
df = dh.read()
dh.overwrite(df)
```

This code assumes that there exists a file `/my/config/files/stuff.yml` like:
```yaml
MyDb:
  name: TestDb{ID}
  path: /tmp/testdb{ID}

MyTblId:
  name: TestDb{ID}.testTbl
  path: /tmp/testdb{ID}/testTbl
```

The `{ID}` parts are either replaced with an empty string (production) or with a uuid
if `tc.reset(debug=True)` has been set.
