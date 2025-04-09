import copy
import io
import os

import pandas as pd
import pyodbc
import sqlalchemy as sa
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import InterfaceError, OperationalError, ProgrammingError
from sqlalchemy_utils import create_database
from sqlalchemy_utils.functions.orm import quote

import els.pd as pn
import els.sa as sq
import els.xl as xl

default_target: dict[str, pd.DataFrame] = {}
open_files: dict[str, io.BytesIO] = {}
open_workbooks: dict[str, xl.ExcelIO] = {}
open_dicts: dict[int, pn.DataFrameDictIO] = {}
open_sa_engs: dict[str, sa.Engine] = {}
open_sqls: dict[str, sq.SQLDBContainer] = {}


supported_mssql_odbc_drivers = {
    "sql server native client 11.0",
    "odbc driver 17 for sql server",
    "odbc driver 18 for sql server",
}


def available_odbc_drivers():
    available = pyodbc.drivers()
    lcased = {v.lower() for v in available}
    return lcased


def supported_available_odbc_drivers():
    supported = supported_mssql_odbc_drivers
    available = available_odbc_drivers()
    return supported.intersection(available)


def fetch_sql_container(url: str, replace: bool = False) -> sq.SQLDBContainer:
    if url is None:
        raise Exception("Cannot fetch None url")
    elif url in open_sqls:
        res = open_sqls[url]
    else:
        res = sq.SQLDBContainer(url, replace)
    open_sqls[url] = res
    return res


def _get_scalar_result(engine, sql):
    with engine.connect() as conn:
        return conn.scalar(sql)


# def database_exists(url):
#     # text = "SELECT 1;"
#     # res = False
#     # eng = sa.create_engine(url)
#     # with eng.connect() as conn:
#     #     res = bool(conn.scalar(sa.text(text)))
#     # eng.dispose()
#     # return res
#     res = False
#     # try:
#     engine = sa.create_engine(url)
#     cn = engine.connect()
#     cn.
#     cn.detach()
#     cn.close()
#     time.sleep(10)
#     # engine.
#     # with engine:
#     #     pass

#     # cn.connection.close()

#     # cn.close()
#     # del cn
#     # if not cn.closed:
#     #     print("XXX not closed")
#     res = True
#     # except Exception:
#     #     pass
#     # finally:
#     #     engine.dispose()
#     return res


def _set_url_database(url: sa.engine.url.URL, database):
    """Set the database of an engine URL.

    :param url: A SQLAlchemy engine URL.
    :param database: New database to set.

    """
    if hasattr(url, "_replace"):
        # Cannot use URL.set() as database may need to be set to None.
        ret = url._replace(database=database)
    else:  # SQLAlchemy <1.4
        url = copy(url)
        url.database = database
        ret = url
    assert ret.database == database, ret
    return ret


def drop_database(url):
    """Issue the appropriate DROP DATABASE statement.

    :param url: A SQLAlchemy engine URL.

    Works similar to the :ref:`create_database` method in that both url text
    and a constructed url are accepted. ::

        drop_database('postgresql://postgres@localhost/name')
        drop_database(engine.url)

    """

    url = make_url(url)
    database = url.database
    dialect_name = url.get_dialect().name
    dialect_driver = url.get_dialect().driver

    if dialect_name == "postgresql":
        url = _set_url_database(url, database="postgres")
    elif dialect_name == "mssql":
        url = _set_url_database(url, database="master")
    elif dialect_name == "cockroachdb":
        url = _set_url_database(url, database="defaultdb")
    elif not dialect_name == "sqlite":
        url = _set_url_database(url, database=None)

    if dialect_name == "mssql" and dialect_driver in {"pymssql", "pyodbc"}:
        engine = sa.create_engine(url, connect_args={"autocommit": True})
    elif dialect_name == "postgresql" and dialect_driver in {
        "asyncpg",
        "pg8000",
        "psycopg",
        "psycopg2",
        "psycopg2cffi",
    }:
        engine = sa.create_engine(url, isolation_level="AUTOCOMMIT")
    else:
        engine = sa.create_engine(url)

    if dialect_name == "sqlite" and database != ":memory:":
        if database:
            os.remove(database)
    elif dialect_name == "postgresql":
        with engine.begin() as conn:
            # Disconnect all users from the database we are dropping.
            version = conn.dialect.server_version_info
            pid_column = "pid" if (version >= (9, 2)) else "procpid"
            text = """
            SELECT pg_terminate_backend(pg_stat_activity.{pid_column})
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{database}'
            AND {pid_column} <> pg_backend_pid();
            """.format(pid_column=pid_column, database=database)
            conn.execute(sa.text(text))

            # Drop the database.
            text = f"DROP DATABASE {quote(conn, database)}"
            conn.execute(sa.text(text))
    else:
        with engine.begin() as conn:
            # text = f"DROP DATABASE {quote(conn, database)}"
            # TODO, seems SINGLE_USER/ROLLBACK call only required on Windows macnines
            text = f"ALTER DATABASE {quote(conn, database)} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;DROP DATABASE {quote(conn, database)}"
            conn.execute(sa.text(text))

    engine.dispose()


def _sqlite_file_exists(database):
    if not os.path.isfile(database) or os.path.getsize(database) < 100:
        return False

    with open(database, "rb") as f:
        header = f.read(100)

    return header[:16] == b"SQLite format 3\x00"


def database_exists(url):
    """Check if a database exists.

    :param url: A SQLAlchemy engine URL.

    Performs backend-specific testing to quickly determine if a database
    exists on the server. ::

        database_exists('postgresql://postgres@localhost/name')  #=> False
        create_database('postgresql://postgres@localhost/name')
        database_exists('postgresql://postgres@localhost/name')  #=> True

    Supports checking against a constructed URL as well. ::

        engine = create_engine('postgresql://postgres@localhost/name')
        database_exists(engine.url)  #=> False
        create_database(engine.url)
        database_exists(engine.url)  #=> True

    """

    url = make_url(url)
    database = url.database
    dialect_name = url.get_dialect().name
    engine = None
    try:
        if dialect_name == "postgresql":
            text = "SELECT 1 FROM pg_database WHERE datname='%s'" % database
            for db in (database, "postgres", "template1", "template0", None):
                url = _set_url_database(url, database=db)
                engine = sa.create_engine(url)
                try:
                    return bool(_get_scalar_result(engine, sa.text(text)))
                except (ProgrammingError, OperationalError):
                    pass
            return False

        elif dialect_name == "mysql":
            url = _set_url_database(url, database=None)
            engine = sa.create_engine(url)
            text = (
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME = '%s'" % database
            )
            return bool(_get_scalar_result(engine, sa.text(text)))

        elif dialect_name == "sqlite":
            url = _set_url_database(url, database=None)
            engine = sa.create_engine(url)
            if database:
                return database == ":memory:" or _sqlite_file_exists(database)
            else:
                # The default SQLAlchemy database is in memory, and :memory: is
                # not required, thus we should support that use case.
                return True
        else:
            text = "SELECT 1"
            try:
                engine = sa.create_engine(url)
                return bool(_get_scalar_result(engine, sa.text(text)))
            except (ProgrammingError, OperationalError, InterfaceError):
                return False
    finally:
        if engine:
            engine.dispose()


def fetch_sa_engine(url, replace: bool = False) -> sa.Engine:
    dialect = url.split(":")[0]
    kwargs = {}
    if dialect in ("mssql+pyodbc", "mssql+") and len(
        supported_available_odbc_drivers()
    ):
        kwargs["fast_executemany"] = True

    if url is None:
        raise Exception("Cannot fetch None url")
    elif url in open_sa_engs:
        res = open_sa_engs[url]
    else:
        if not database_exists(url):
            create_database(url)
        elif replace:
            drop_database(url)
            # with sa.engine(url).connect() as cn:
            #     cn.execute()
            create_database(url)
        res = sa.create_engine(url, **kwargs)

    open_sa_engs[url] = res
    return res


def urlize_dict(df_dict: dict):
    fetch_df_dict_io(df_dict)
    return f"dict://{id(df_dict)}"


def fetch_df_dict_io(df_dict: dict, replace: bool = False):
    if isinstance(df_dict, int):
        return open_dicts[df_dict]
    if isinstance(df_dict, str):
        return open_dicts[int(df_dict.split("/")[-1])]
    if df_dict is None:
        raise Exception("Cannot fetch None dict")
    elif id(df_dict) in open_dicts:
        res = open_dicts[id(df_dict)]
    else:
        res = pn.DataFrameDictIO(df_dict, replace)
    open_dicts[id(df_dict)] = res
    return res


def fetch_file_io(url: str, replace: bool = False):
    if url is None:
        raise Exception("Cannot fetch None url")
    elif url in open_files:
        res = open_files[url]
    # only allows replacing once:
    elif replace:
        res = io.BytesIO()
    # chck file exists:
    elif os.path.isfile(url):
        with open(url, "rb") as file:
            res = io.BytesIO(file.read())
    else:
        res = io.BytesIO()
    open_files[url] = res
    return res


def fetch_excel_io(url: str, replace: bool = False):
    if url is None:
        raise Exception("Cannot fetch None url")
    elif url in open_workbooks:
        res = open_workbooks[url]
    else:
        res = xl.ExcelIO(url, replace)
    open_workbooks[url] = res
    return res


def listify(v):
    return v if isinstance(v, (list, tuple)) else [v]
