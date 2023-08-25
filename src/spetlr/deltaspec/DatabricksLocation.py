from urllib.parse import urlparse


def standard_databricks_location(val: str) -> str:
    p = urlparse(val)
    if not p.scheme:
        p = p._replace(scheme="dbfs")

    return p.geturl()
