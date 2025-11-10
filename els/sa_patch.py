import struct
import urllib.parse
from itertools import chain, repeat

import sqlalchemy as sa
from azure.identity import DefaultAzureCredential
from sqlalchemy.engine.url import URL


def create_engine(url: URL, **kwargs):
    # TODO can be improved to check for this at end of servername string
    if ".fabric.microsoft.com" in str(url):
        url_split = url.split("/")
        # TODO, makes too many assumptions on structure of url
        sql_endpoint = url_split[2]
        database = url_split[3]

        resource_url = "https://database.windows.net/.default"
        azure_credentials = DefaultAzureCredential()
        token_object = azure_credentials.get_token(resource_url)

        connection_string = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={sql_endpoint},1433;"
            f"Database={database};Encrypt=Yes;TrustServerCertificate=No"
        )
        params = urllib.parse.quote(connection_string)

        token_as_bytes = bytes(token_object.token, "UTF-8")
        encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0))))
        token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes
        attrs_before = {1256: token_bytes}

        sqlalchemy_url = f"mssql+pyodbc:///?odbc_connect={params}"
        return sa.create_engine(
            sqlalchemy_url, connect_args={"attrs_before": attrs_before}, **kwargs
        )
    else:
        return sa.create_engine(url, **kwargs)
