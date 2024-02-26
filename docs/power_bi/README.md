
# PowerBi and PowerBiClient classes

The `PowerBi` and `PowerBiClient` classes contain logic for refreshing
PowerBI datasets and for checking if the last dataset refresh completed
successfully. 

For easier PowerBI credential handling (service principal or AD user),
the first parameter to the `PowerBi` constructor must be a `PowerBiClient`
class object. 

## PowerBI Permissions

To enable PowerBI API access in your PowerBI, you need to enable
the "Service principals can use Fabric APIs" setting (see the screen-shot).
Additionally, you need to specify the user group that should have access 
to the API.

![Power BI admin settings](./admin_settings.png)

Apart from this, each PowerBI dataset should have a user or service principal
attached, that is part of this user group.


## Links

[Register an App and give the needed permissions. A very well how-to-guide can be found here.](https://www.sqlshack.com/how-to-access-power-bi-rest-apis-programmatically/)

[How to Refresh a Power BI Dataset with Python.](https://pbi-guy.com/2022/01/07/refresh-a-power-bi-dataset-with-python/)

### API documentation:

[Datasets - Refresh Dataset In Group](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/refresh-dataset-in-group)

[Datasets - Get Refresh History In Group](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-refresh-history-in-group)


# Usage of PowerBi and PowerBiClient classes

## Step 1: Create PowerBI credentials

The client ID, client secret, and tenant ID values should be stored in a key vault,
and loaded from the key vault or Databricks secrets scope.

```python
# example PowerBiClient credentials object
from spetlr.power_bi.PowerBiClient import PowerBiClient 
from my_proj.env import secrets

class MyPowerBiClient(PowerBiClient):
    def __init__(self):
        super().__init__(
            client_id=secrets.get_power_bi_client(),
            client_secret=secrets.get_power_bi_secret(),
            tenant_id=secrets.get_power_bi_tenant(),
        )
```

## Step 2: List available workspaces

If no workspace parameter is specified, a list of available workspaces
is shown using Pandas. This logic can be used in a notebook.

```python
# example listing of available workspaces
from spetlr.power_bi.PowerBi import PowerBi 

client = MyPowerBiClient()
PowerBi(client).check()
```

```
Available workspaces:   
+----+--------------------------------------+----------------+
|    |            workspace_id              | workspace_name |
+----+--------------------------------------+----------------+
|   1| 614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0 | Finance        |
|   2| 5da990e9-089e-472c-a7fa-4fc3dd096d01 | CRM            |
+----+--------------------------------------+----------------+
```

## Step 3: List available datasets

If no dataset parameter is specified, a list of available datasets
in the given workspace is shown using Pandas.
This logic can be used in a notebook.

```python
# example listing of available datasets
from spetlr.power_bi.PowerBi import PowerBi 

client = MyPowerBiClient()
PowerBi(client, workspace_name="Finance").check()

# alternatively:
PowerBi(client, workspace_id="614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0").check()
```

```
Available datasets:
+----+--------------------------------------+----------------+
|    |             dataset_id               |  dataset_name  |
+----+--------------------------------------+----------------+
|   1| b1f0a07e-e348-402c-a2b2-11f3e31181ce | Invoicing      |
|   3| 4de28a6f-f7d4-4186-a529-bf6c65e67b31 | Fees           |
|   2| 2e848e9a-47a3-4b0e-a22a-af35507ec8c4 | Reimbursement  |
+----+--------------------------------------+----------------+
```

## Step 4: Check the status and time of the last refresh of a given dataset

The check() method can be used to check the status and time of the last
refresh of a dataset. An exception will be cast if the last refresh failed,
or if the last refresh finished more the given number of minutes ago.
The number of minutes can be specified in the optional
"max_minutes_after_last_refresh" parameter (default is 12 hours).

```python
# example last refresh time checking
from spetlr.power_bi.PowerBi import PowerBi 

client = MyPowerBiClient()
PowerBi(client, workspace_name="Finance", dataset_name="Invoicing",
        max_minutes_after_last_refresh=2*60).check()

# alternatively:
PowerBi(client, workspace_id="614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
        dataset_id="b1f0a07e-e348-402c-a2b2-11f3e31181ce").check()
```

```
Refresh completed successfully at 2024-02-01 10:15 (local time).
True   
```

## Step 5: Start a new refresh of a given dataset without waiting

The start_refresh() method starts a new refresh of the given PowerBI
dataset asynchronously. You need to call the check() method after waiting
for some sufficient time (e.g. from a separate monitoring job) to verify
if the refresh succeeded.

```python
# example starting of a dataset refresh
from spetlr.power_bi.PowerBi import PowerBi 

client = MyPowerBiClient()
PowerBi(client, workspace_name="Finance", dataset_name="Invoicing").start_refresh()

# alternatively:
PowerBi(client, workspace_id="614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
        dataset_id="b1f0a07e-e348-402c-a2b2-11f3e31181ce").start_refresh()
```

```
A new refresh has been successfully triggered.
True
```

## Step 6: Start a new refresh of a given dataset and wait for the result

The refresh() method starts a new refresh of the given PowerBI dataset
synchronously. It waits until the refresh is finished or until a time-out
occurs. The time-out can be specified using the "timeout_in_seconds" parameter.
If the refresh fails or a time-out occurs, the method casts an exception.

The wait time is synchronized with the execution time of the previous refresh,
making sure as few requests to the PowerBI API would be made as possible,
while ensuring the method would finish as soon as possible. 

```python
# example starting of a dataset refresh
from spetlr.power_bi.PowerBi import PowerBi 

client = MyPowerBiClient()
PowerBi(client, workspace_name="Finance", dataset_name="Invoicing",
        timeout_in_seconds=10*60).refresh()

# alternatively:
PowerBi(client, workspace_id="614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
        dataset_id="b1f0a07e-e348-402c-a2b2-11f3e31181ce").refresh()
```

```
A new refresh has been successfully triggered.
Waiting 60 seconds...
Waiting 15 seconds...
Refresh completed successfully at 2024-02-02 09:02 (local time).
True
```