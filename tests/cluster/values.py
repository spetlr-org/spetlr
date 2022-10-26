from functools import cache

from atc.functions import init_dbutils


@cache
def getValue(secret_name: str):
    return init_dbutils().secrets.get("values", secret_name)


def resourceName():
    return getValue("resourceName")
