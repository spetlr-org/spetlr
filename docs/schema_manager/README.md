## The Schema Manager Class
The Schema Manager class is the intended way to retrieve schemas in atc. It can be desirable to retrieve the schemas of dataframes at various times, for example to ensure that the data has the correct form before being written to a table or to be able to reason on the columns included in incoming data.

The `SchemaManager` class is a singleton class that can be invoked anywhere to get any schema defined in a dataplatform using offline mthods.

### Summary
This section will briefly cover the included methods and their use, the following sections go into slightly more detail with how they are implemented.
The main methods are:
- `register_schema`
- `get_schema`
- `get_all_schemas`
Schemas written in pyspark must be registered with a given name to be accessible. The schema of any table can be retrieved by invoking the `get_schema` method with either the given name of the schema or the name of the table the schema is attributed to in a .yaml file.
Lastly, all defined schemas can be retrieved in a single dictionary. This includes those defined in separate files as well as .yaml files.


### Schema Types
Schemas can be defined using various types and in various locations in the platform. Currently, the schema manager supports:
- Pyspark StructType definitions
- Spark SQL definitions

Schemas are written directly to the .yaml files under the `schema` attribute, with the exception of pyspark schemas, which are defined in separate files. For pyspark schemas to be available, they must be registered in the Schema Manager under a name with the `register_schema` function.

### Communication with the Configurator
Schemas defined directly in .yaml files are made available through the configurator. The `get_schema`-method of the schema manager works as follows:
1. Look for the given name in the registered schemas.
2. If not found, query the Configurator for the `schema` attribute of a table with the given name.
3. If the result is a string, it must be referencing a pyspark schema, which should be registered.
4. Otherwise, it must be a dict with the schema defined in the .yaml file and both the schema and its type can be found here.

Similarly, the `get_all_schemas`-method iterates over the Configurators `table_details` to find all tables that have a `schema` attribute.
