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


## Authentication modes
### Key-auth (Account Key)

This is a single “master” key for the account.

Packages you use:

* Data plane: azure.cosmos.CosmosClient(credential=<account_key>)

* Spark connector: set spark.cosmos.accountKey=<key>

We can do everything on the data plane with that key—items, queries, and also database/container/throughput (“offers”) operations via the data plane.


### AAD-auth (Service Principal)

This is a token-based access via Entra ID (Azure AD) with RBAC.

Packages you use:

* Token: azure.identity.ClientSecretCredential(...)

* Data plane: azure.cosmos.CosmosClient(credential=<TokenCredential>)

* Management plane (a.k.a. control plane / ARM):
azure.mgmt.cosmosdb.CosmosDBManagementClient(credential=<TokenCredential>, subscription_id=...)

* Spark connector: set spark.cosmos.auth.type=ServicePrincipal plus tenant/clientId/clientSecret

What you can do:

* Data plane (RBAC-scoped): items & queries (read/write/delete documents).

* Management plane (ARM): create/delete databases/containers, and manage throughput.
(These are not allowed via AAD on the data plane; use the management SDK.)


## The planes
* Data plane = working inside a database/container (items, queries, bulk ingest).
* SDK: azure.cosmos (and the Spark connector).
* Management (control) plane / ARM = managing the resources (accounts, databases, containers, throughput, backup, etc.).
SDK: azure.mgmt.cosmosdb.

When to pick which

* Use Key-auth for quick setups/tests where a single secret is acceptable.

* Use AAD for production: grant Cosmos DB Built-in Data Contributor (data plane) to the SPN, and a Cosmos DB Operator/Contributor role (management plane) where you need to create/delete resources or change RU/throughput.

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
    subscription_id="sub-id",
    resource_group="rg-name" 
)

# Read a container into Spark
df = db.read_table_by_name("tablename")

# Write a DataFrame to a container
db.write_table_by_name(df, "tablename")

# (Optional) create DB/containers through the SDK
db.create_database()
db.create_table(table_name="tablename", partition_key="/id", offer_throughput=1100)

```
