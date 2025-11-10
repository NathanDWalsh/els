from copy import copy
from typing import Union

import sqlalchemy as sa
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy_utils.functions.database import (
    _get_scalar_result,
    _set_url_database,
    _sqlite_file_exists,
    quote,
)

from .sa_patch import create_engine


def database_exists(url: Union[URL, str]) -> bool:
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

    if ".fabric.microsoft.com" in url:
        return True

    url = make_url(url)
    database = url.database
    dialect_name = url.get_dialect().name
    engine = None
    try:
        if dialect_name == "postgresql":
            text = "SELECT 1 FROM pg_database WHERE datname='%s'" % database
            for db in (database, "postgres", "template1", "template0", None):
                url = _set_url_database(url, database=db)
                engine = create_engine(url)
                try:
                    return bool(_get_scalar_result(engine, sa.text(text)))
                except (ProgrammingError, OperationalError):
                    pass
            return False

        elif dialect_name == "mysql":
            url = _set_url_database(url, database=None)
            engine = create_engine(url)
            text = (
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME = '%s'" % database
            )
            return bool(_get_scalar_result(engine, sa.text(text)))

        elif dialect_name == "sqlite":
            url = _set_url_database(url, database=None)
            engine = create_engine(url)
            if database:
                return database == ":memory:" or _sqlite_file_exists(database)
            else:
                # The default SQLAlchemy database is in memory, and :memory: is
                # not required, thus we should support that use case.
                return True
        elif dialect_name == "mssql":
            text = f"select 1 from sys.databases where name = '{url.database}'"
            url_master = copy(url)
            url_master = _set_url_database(url, database="master")
            try:
                engine = create_engine(url_master)
                return bool(_get_scalar_result(engine, sa.text(text)))
            except Exception:
                return False
        else:
            text = "SELECT 1"
            try:
                engine = create_engine(url)
                return _get_scalar_result(engine, sa.text(text))
            except (ProgrammingError, OperationalError):
                return False

    finally:
        if engine:
            engine.dispose()


def create_database(url, encoding="utf8", template=None):
    """Issue the appropriate CREATE DATABASE statement.

    :param url: A SQLAlchemy engine URL.
    :param encoding: The encoding to create the database as.
    :param template:
        The name of the template from which to create the new database. At the
        moment only supported by PostgreSQL driver.

    To create a database, you can pass a simple URL that would have
    been passed to ``create_engine``. ::

        create_database('postgresql://postgres@localhost/name')

    You may also pass the url from an existing engine. ::

        create_database(engine.url)

    Has full support for mysql, postgres, and sqlite. In theory,
    other database engines should be supported.
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

    if (dialect_name == "mssql" and dialect_driver in {"pymssql", "pyodbc"}) or (
        dialect_name == "postgresql"
        and dialect_driver
        in {"asyncpg", "pg8000", "psycopg", "psycopg2", "psycopg2cffi"}
    ):
        engine = create_engine(url, isolation_level="AUTOCOMMIT")
    else:
        engine = create_engine(url)

    if dialect_name == "postgresql":
        if not template:
            template = "template1"

        with engine.begin() as conn:
            text = "CREATE DATABASE {} ENCODING '{}' TEMPLATE {}".format(
                quote(conn, database), encoding, quote(conn, template)
            )
            conn.execute(sa.text(text))

    elif dialect_name == "mysql":
        with engine.begin() as conn:
            text = "CREATE DATABASE {} CHARACTER SET = '{}'".format(
                quote(conn, database), encoding
            )
            conn.execute(sa.text(text))

    elif dialect_name == "sqlite" and database != ":memory:":
        if database:
            with engine.begin() as conn:
                conn.execute(sa.text("CREATE TABLE DB(id int)"))
                conn.execute(sa.text("DROP TABLE DB"))

    else:
        with engine.begin() as conn:
            text = f"CREATE DATABASE {quote(conn, database)}"
            conn.execute(sa.text(text))

    engine.dispose()
