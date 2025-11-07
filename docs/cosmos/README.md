# CosmosDb class

A tiny convenience wrapper around the Azure Cosmos DB Spark connector (cosmos.oltp) and the Python SDK, letting you read/write containers via Spark and manage containers via the SDK. Supports either Account Key or Azure AD (service principal) authentication.

## Prereqs

* Python packages: azure-cosmos (always), azure-identity (for AAD).

* Spark connector artifact matching your Spark version, e.g. com.azure.cosmos.spark:azure-cosmos-spark_3-4_2-12:<version>

* Data-plane permissions:

  * Account Key: key must be valid for the account.

  * AAD: service principal must have Cosmos DB data-plane role(s) (e.g., Data Reader/Contributor) on the target DB/container.

  * Endpoint: provide either account_name or full endpoint URL.

  * Note: Connector options are case-sensitive (spark.cosmos.accountKey with capital K).

## Example: Account key

```python

from spetlr.cosmos import CosmosDb

db = CosmosDb(
    account_key="KEY123",
    database="something",
    account_name="mycosmosacct",  # or endpoint="https://mycosmosacct.documents.azure.com:443/"
)

# Read a container into Spark
df = db.read_table_by_name("tablename")

# Write a DataFrame to a container
db.write_table_by_name(df, "tablename")

# (Optional) DDL via Cosmos Spark Catalog (not UC-compatible)
db.execute_sql("""
CREATE TABLE IF NOT EXISTS cosmosCatalog.something.tablename
USING cosmos.oltp
TBLPROPERTIES(partitionKeyPath='/id', manualThroughput='1100')
""")
```

## Example: AAD Service Principal

```python
from spetlr.cosmos import CosmosDb

db = CosmosDb(
    account_key=None,  # switches to AAD mode
    database="something",
    endpoint="https://mycosmosacct.documents.azure.com:443/",
    tenant_id="00000000-0000-0000-0000-000000000000",
    client_id="11111111-1111-1111-1111-111111111111",
    client_secret="super-secret",
)

# Read a container into Spark
df = db.read_table_by_name("tablename")

# Write a DataFrame to a container
db.write_table_by_name(df, "tablename")

# (Optional) create DB/containers through the SDK
db.create_database()
db.create_table(table_name="tablename", partition_key="/id", offer_throughput=1100)

```