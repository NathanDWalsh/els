from copy import copy

import sqlalchemy as sa
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy_utils.functions.database import (
    _get_scalar_result,
    _set_url_database,
    _sqlite_file_exists,
)  # type: ignore


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
        elif dialect_name == "mssql":
            text = f"select 1 from sys.databases where name = '{url.database}'"
            url_master = copy(url)
            url_master = _set_url_database(url, database="master")
            try:
                engine = sa.create_engine(url_master)
                return bool(_get_scalar_result(engine, sa.text(text)))
            except Exception:
                return False
        else:
            text = "SELECT 1"
            try:
                engine = sa.create_engine(url)
                return _get_scalar_result(engine, sa.text(text))
            except (ProgrammingError, OperationalError):
                return False

    finally:
        if engine:
            engine.dispose()
