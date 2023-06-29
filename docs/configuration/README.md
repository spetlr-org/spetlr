
# Table Configurator

## Motivation
In databricks there is one standard way to refer to a table, the 
"database.table" reference. In our dataplatform projects we have often 
found a need for a more abstracted notion of a table reference,
one that allows for injection of test versions of tables and that allows 
unified reference across different environments (dev, staging, prod).
The `Configurator` provides such a further abstraction level.

## Setting up configuration

The `Configurator` is a singleton class that you need to instantiate
and configure once in your project. Make sure this code is always executed
before any reference to tables in made. Example:
```python
from spetlr import Configurator
tc = Configurator()
tc.register('ENV','myenv')
tc.add_resource_path(my.resource.module)
```

### Configuration from yaml or json

The `Configurator` can be configured with json or yaml files. The
files must contain the following structure:
- top level objects are resources
- each resource must have one of these shapes:
  - a string (can contain substitutions, see below)
  - a dict with a single key named 'alias' to refer to another resource
  - a dict with two keys called 'release' and 'debug', each containing resource details
  - resource details consisting of a dictionary of attributes, in the case of 
    describing a hive table or database, a certain scheme needs to be followed as 
    described below. However, it is possible to use any structure to describe 
    resources of any kind.
 
The following is an example of a configuration file
```yaml
MyFirst:
  name: first{ID}
  path: /{MNT}/my/first{ID}
# MNT and ID are default replacements
# using .set_prod(), MNT becomes 'mnt', while in debug it becomes tmp
# is helps when using mount point locations for tables.

MyAlias:
  alias: MyFirst

MyForked:
  release:
    alias: MyFirst
  debug:
    name: another
    path: /my/temp/path
# using release and debug is an alternative way of differentiating the cases instead
# of using {ID}, note that {ID} has the advantage of separating concurrent test runs

MyRecursing:
  name: recursing{ID}
  path: /{MNT}/tables/{MyRecursing_name}
# using the notation '_name' refers back to the 'name' property of that dictionary.
# alias and release/debug will be resolved before the property is accessed
```

You optionally either provide 'release' and 'debug' versions of each table
or include the structure `{ID}` in the name and path. This is a special
replacement string that will be replaced with and empty string for 
production tables, or with a "__{GUID}" string when debug is selected.
The guid construction allows for non-colliding parallel testing.

Beyond the resource definitions, the `Configurator` needs to be 
configured to return production or test versions of tables this is done
at the start of your code. In your jobs you need to set `Configurator().set_prod()`
whereas your unit-tests should call `Configurator().set_debug()`.

For production: set this in the `__init__.py` where the .yml files is located: 

```python
from spetlr import Configurator

# config is your dataplatform
# configurations that you define
from dataplatform.env import config, databases, config

def init_configurator():
    c = Configurator()
    c.register("ENV", config.environment.environment_name.lower())
    c.add_resource_path(table_names)
    c.add_sql_resource_path(databases)
    return c
```

For testing create this (kind of) function:

```python
from spetlr import Configurator
from dataplatform.env.table_names import init_configurator

# Maybe import some test databases and tables
from . import databases


def debug_configurator():
    c = init_configurator()
    c.add_sql_resource_path(databases)
    c.set_debug()
    return c

```

In the tests you then use the debug_configurator:

```python
from dataplatform.test import debug_configurator
from spetlrtools.testing import DataframeTestCase

class ExampleTests(DataframeTestCase):
    @classmethod
    def setUpClass(cls):
        debug_configurator()

    ...
```

### IntelliSense support

It is possible to skip the use of yaml or sql files and to set all the 
configurations using the API command `.register()` or `.define()`.
Both of these return a string key which can be used in the following ways
```python
from spetlr import Configurator, delta
c = Configurator()

tbl = c.define(name="ByDb.Table", path="/mnt/{ENV}/path/to/table")

dh = delta.DeltaHandle.from_tc(tbl)
```

The highly useful property of this approach in modern IDEs is that the definition of 
`tbl` can be viewed in context highlighting for the object `tbl` and it its possible 
to jump straight to the definition of the table from any code that deals with the key.

The case of `.define()` takes this approach even further by auto-generating a key 
that will never be used or viewed manually anyway. Together with it the `.key_of()` 
method allows the user to recover the key of an entry provided any property value is 
known.
```python
from spetlr import Configurator
c = Configurator()
tbl = c.define(name="ByDb.Table", path="/mnt/{ENV}/path/to/table")
assert tbl == c.key_of("name", "ByDb.Table")
```

### String substitutions

As was already seen in the example above, all strings can contain python 
format-string placeholders that reference other strings known to the configurator.
The following rules apply:
- Any value returned by the configurator will be fully resolved, meaning:
  - all alias and release/debug redirections have been followed
  - all `{}` substitutions have been applied
  - since recursive substitutions are allowed, literal `{` or `}` can never be returned
  - a stack is maintained internally to detect reference loops
- any plain reference, like `{MyRecursing}` will resolve to either the entity itself 
  (if it is a string), or to the `name` key of the fully resolved resource detail. .
- any reference containing an underscore like `{MyRecursing_name}` will consider the 
  part after the underscore to be a dictionary key and will return the fully resolved 
  value. References to other parts of the same resource are allowed.

### Describing Hive tables

In spark sql, there are [database create statements](https://spark.apache.org/docs/3.0.0-preview/sql-ref-syntax-ddl-create-database.html)
and [table create statemets](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html)
that follow a fixed pattern reproduced here:

```sparksql
CREATE {DATABASE | SCHEMA} [ IF NOT EXISTS ] database_name
  [ COMMENT database_comment ]
  [ LOCATION database_directory ]
  [ WITH DBPROPERTIES (property_name=property_value [ , ...]) ];

CREATE TABLE [ IF NOT EXISTS ] table_identifier
    [ ( col_name1 col_type1 [ COMMENT col_comment1 ], ... ) ]
    USING data_source
    [ OPTIONS ( key1=val1, key2=val2, ... ) ]
    [ PARTITIONED BY ( col_name1, col_name2, ... ) ]
    [ CLUSTERED BY ( col_name3, col_name4, ... ) 
        [ SORTED BY ( col_name [ ASC | DESC ], ... ) ] 
        INTO num_buckets BUCKETS ]
    [ LOCATION path ]
    [ COMMENT table_comment ]
    [ TBLPROPERTIES ( key1=val1, key2=val2, ... ) ]
    [ AS select_statement ]
```

The logic in this library expects the information in the above statements to be 
reproduced in yaml in the following structure:
```yaml
MySpetlrDbReference:
  name: database_name
  comment: database_comment
  path: database_directory
  format: "db"
  dbproperties:
    property_name: property_value

MySpetlrTableReference:
  name: table_identifier
  path: path
  format: data_source
  schema:
    sql: |
      col_name1 col_type1 [ COMMENT col_comment1 ], ... 
  options:
    key1: val1
    key2: val2
  partitioned_by:
    - col_name1
    - col_name2
  clustered_by:
    cols:
      - col_name3
      - col_name4 
    buckets: num_buckets
    sorted_by:
      - name: col_name 
        ordering: "ASC" or "DESC"
  comment: table_comment
  tblproperties:
    key1: val1
    key2: val2
```

Note that the form `CREATE TABLE table_identifier AS select_statement` is not supported.
For alternate ways to specify the schema, see the documentation for the schema manager.

### Configuration from sql

When using sql statements to create and manage tables, a developer may desire to 
keep all information referring to a table within one file. Therefore, the 
configurator employs a sql parsing library to extract table details directly from the 
CREATE statements. It can be used like this:

```python
from . import my_sql_folder
Configurator().add_sql_resource_path(my_sql_folder)
```

A sql CREATE statement already contains all information that the configurator needs 
except the key, aka. table_id, that shall be used to refer to the table.
This key needs to be added to the sql in the form of a comment with a magic prefix
`"-- spetlr.Configurator "` (note the spaces. not case-sensitive.) See the following 
example:
```sparksql
-- spetlr.Configurator key: MySparkDb
CREATE DATABASE IF NOT EXISTS `my_db1{ID}`
COMMENT "Dummy Database 1"
LOCATION "/tmp/foo/bar/my_db1/";

-- spetlr.Configurator key: MyDetailsTable
CREATE TABLE IF NOT EXISTS `my_db1{ID}.details`
(
  {MyAlias_schema},
  another int
  -- comment with ;
)
USING DELTA
COMMENT "Dummy Database 1 details"
LOCATION "/{MNT}/foo/bar/my_db1/details/";

-- pure configurator magic in this statement
-- spetlr.Configurator key: MyAlias
-- spetlr.Configurator alias: MySqlTable
;


-- SPETLR.CONFIGURATOR key: MySqlTable
-- spetlr.Configurator delete_on_delta_schema_mismatch: true
CREATE TABLE IF NOT EXISTS `{MySparkDb}.tbl1`
(
  a int,
  b int,
  c string,
  d timestamp
)
USING DELTA
COMMENT "Dummy Database 1 table 1"
LOCATION "/{MNT}/foo/bar/my_db1/tbl1/";
```

The example is quite complex and demonstrates a number of features:
- the magic tag "-- SPETLR.CONFIGURATOR " is case-insensitive. The part after the tag 
  is case-sensitive!
- the key has to be specified as `-- SPETLR.CONFIGURATOR key: MyKey`.
- any other attribute can be specified after the same magic tag and will be 
  available through the configurator. In fact, all comments with the magic tag will 
  be collected together and will be interpreted as a `yaml` document, so more 
  complicated structures are also possible.
- all python string substitutions that the configurator offers are also available 
  here in the sql configuration.
- string substitution is available inside the schema, this allows schemas to be 
  constructed from other schemas.

When using the configurator to parse `sql` code, the in-memory structure will be as 
described in the section on hive-table specification.

## Using the Configurator

Once configured, the table configurator is often not used directly.
Other classes in the spetlr framework use it when configured with a resource
ID. You can find examples in the eventhub unittests:
```python
from spetlr.eh import EventHubCapture
EventHubCapture.from_tc("SpetlrEh")
```
or in the delta handle unit-tests:
```python
from spetlr.delta import DeltaHandle
DeltaHandle.from_tc("MyTbl")
```
But sometimes you still need to call the table configurator methods
e.g. when constructing your own sql:
```python
from spetlr.config_master import Configurator
f"MERGE INTO {Configurator().table_name('MyTbl')} AS target ..."
```

## Further Features

### MNT key
'MNT' is another special replacement, similar to "ID". In production, it
is replaced with the string 'mnt' while in debug it is replaced with 'tmp'.
The intended usage is in paths where production tables are mounted on
external storage, typically mounted under "/mnt" whereas test tables 
should be written to "/tmp" you can use is as in this example:
```yaml
MyTable:
  name: mydb{ID}.data_table
  path: /{MNT}/somestorage{ENV}/mydb{ID}/data_table
```

### Extra details
Extra details are now deprecated. Simply register your extras as simple string resources.

```python
from spetlr.config_master import Configurator
tc = Configurator()
tc.register('ENV', 'prod')
```


## The Configurator Command Line Interface (CLI)

The table configurations, available to the configurator can be useful when executing 
actions on the command line. See below for individual commands. To expose the 
configurator command line interface, you need to call the `.cli()` method *after* 
you have initialized the configurator with your project details. You therefor need 
to expose a command line script as shown below.

In the file that initializes your configurator:
```
def init_my_configurator():
    c= Configurator()
    c.add_resource_path(my_yaml_module)
    c.register('ENV', config.my_env_name)
    return c

if __name__ == "__main__":
    init_my_configurator().cli()
```
Now, all the functionality below will become available on the command line.

### Generated Keys File
When using an IDE to develop python code, a useful feature is auto-completion and 
linting. Such features are however not available when using string keys from yaml 
files. It can therefore be useful to extract the keys from the yaml configurations 
and make them available as python objects.

Call the following command to generate such a keys file from your initialized 
configurator.

```
$> my_config generate-keys-file -o keys.py
```

This will create the file `keys.py` with the following example contents:
```python
# AUTO GENERATED FILE.
# contains all spetlr.Configurator keys

MyFirst = "MyFirst"
MySecond = "MySecond"
MyAlias = "MyAlias"
MyForked = "MyForked"
MyRecursing = "MyRecursing"
```

You can now use the keys file to auto-complete and validate your yaml keys:
```python
DeltaHandle.from_tc("MyFirst")
```
becomes
```python
from keys import MyFirst
DeltaHandle.from_tc(MyFirst)
```
which also supports flake8 linting for correct spelling.

If you want to check that you did not forget to update the keys file as part of your 
CICD pipeline, running the same command with the additional option `--check-only` 
will return an exit code of 0 if the file was already up-to-date and 1 otherwise.
