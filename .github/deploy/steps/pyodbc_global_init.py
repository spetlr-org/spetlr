



import base64
import time
import sys

from databricks.sdk import WorkspaceClient


workspaceURL = sys.argv[1]

bearerToken = sys.argv[1]


w = WorkspaceClient(??)


sh_query ="sudo ACCEPT_EULA=Y apt-get install -q -y /dbfs/databricks/drivers/msodbcsql17_amd64.deb"


created = w.global_init_scripts.create(name=f'PyODBC init',
                                       script=base64.b64encode((sh_query).encode()).decode(),
                                       enabled=True,
                                       position=10)

# cleanup
#w.global_init_scripts.delete(delete=created.script_id)