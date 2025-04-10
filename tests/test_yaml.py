import collections
import os
import random

import pandas as pd
import pytest
import yaml
from faker import Faker

import els.core as el
from els.cli import execute
from els.config import Config

_Test = collections.namedtuple("_Test", ["name", "df", "kwargs"])


# Get the ec.Config dictionary for a given DataFrame
def get_df_config(df: pd.DataFrame):
    config_df = Config()
    config_df.source.dtype = df.dtypes.apply(lambda x: x.name).to_dict()
    for k, v in config_df.source.dtype.items():
        if v.split("[")[0] == "datetime64":
            config_df.source.dtype[k] = "date"
    return config_df


def test_enum_conversion():
    config = Config()
    assert config.target.consistency == "strict"


# Test python type to pandas type conversion and euqality
@pytest.mark.parametrize(
    "py_val, dtype",
    [
        (1, pd.Int64Dtype),
        (1.1, pd.Float64Dtype),
        (True, pd.BooleanDtype),
        ("a", pd.StringDtype),
    ],
)
def test_pd_type_equality(py_val, dtype):
    # pandas type should be the same as the python type
    assert type(pd.array([py_val]).dtype) is dtype


def get_atomic_strings():
    res = {
        "Stringy": "a",
        "Inty": "1",
        "Floaty": "1.1",
        "Booly": "True",
        "Datey": "2021-01-01",
        "Datetimey": "2021-01-01 00:00:00",
        "Nully": None,
        # These will always be imported as NA
        # "Empty": "",
        # "Noney": "None",
        # "Nully": "null",
        # "Nany": "NaN",
    }

    return res


def get_atomic_times():
    res = {
        "ISODate": "2000-01-01",
        "ISODateTime": "2000-01-01T22:22:22",
        # TODO/known issue: loss of precision in thousands of seconds in mssql
        # "ISODateTimeMs": "2000-01-01T22:22:22.222", # fails in mssql
        "ISODateTimeMs": "2000-01-01T22:22:22.22",
        "ISOTime": "22:22:22",
        "ISOTimeMs": "22:22:22.22",
    }

    return res


def get_atomic_string_frames():
    res = {
        f"{pd.StringDtype.name.capitalize()}({k})": pd.DataFrame(
            {k: [v]}, dtype=pd.StringDtype.name
        )
        for k, v in get_atomic_strings().items()
    }
    return res


def get_atomic_number_frames():
    res = {
        f"{num_type.name.capitalize()}({num_val})": pd.DataFrame(
            {num_type.__name__: [num_val]}, dtype=num_type.name
        )
        for num_type in [pd.Float64Dtype, pd.Int64Dtype]
        for num_val in [-1, 0, 1, None]
    }
    return res


def get_atomic_time_frames():
    res = {
        f"DATE({k})": pd.DataFrame({k: [v]}, dtype="datetime64[ns]")
        for k, v in get_atomic_times().items()
    }
    return res


def get_faker_frames():
    fake = Faker()
    fake.seed_instance(1)

    # Randomly return a value or None
    def occasionally_null(value, null_probability=0.1):
        return value if random.random() > null_probability else None

    number_of_rows = 100

    sample_data = {
        "id": [
            occasionally_null(fake.unique.random_int(min=1, max=1000000))
            for _ in range(number_of_rows)
        ],
        "name": [occasionally_null(fake.name()) for _ in range(number_of_rows)],
        "email": [occasionally_null(fake.email()) for _ in range(number_of_rows)],
        "address": [occasionally_null(fake.address()) for _ in range(number_of_rows)],
        "hired_at_date": [
            occasionally_null(fake.date()) for _ in range(number_of_rows)
        ],
        "is_active": [fake.boolean() for _ in range(number_of_rows)],
        "salary": [
            occasionally_null(
                fake.pyfloat(
                    left_digits=6,
                    right_digits=2,
                    positive=True,
                    min_value=60000.0,
                    max_value=600000.0,
                )
            )
            for _ in range(number_of_rows)
        ],
    }

    data_types = {
        "id": pd.Int64Dtype.name,  # Nullable integer type
        "salary": pd.Float64Dtype.name,  # Nullable float type
        "name": pd.StringDtype.name,
        "email": pd.StringDtype.name,
        "address": pd.StringDtype.name,
        "hired_at_date": "datetime64[ns]",
        # "is_active": pd.BooleanDtype.name,  # Nullable boolean type
    }

    df = pd.DataFrame(sample_data).astype(data_types)

    res = {"FakeEmployee100": df}

    return res


def get_atomic_bool_frames():
    res = {
        f"{num_type.name.capitalize()}({num_val})": pd.DataFrame(
            {num_type.__name__: [num_val]}, dtype=num_type.name
        )
        for num_type in [pd.BooleanDtype]
        for num_val in [True, False, None]
    }

    return res


def get_1r1c_tests_csv(atomics: dict):
    test_frames = [
        _Test(
            f"1r1c{name}",
            df,
            {"quoting": quoting},
        )
        for name, df in atomics.items()
        for quoting in [0, 1, 2, 3]
        # single empty field record must be quoted
        if not (quoting == 3 and df.size == 1 and pd.isna(df.iloc[0, 0]))
    ]

    return test_frames


def get_1r1c_tests_excel(atomics: dict):
    test_frames = [
        _Test(
            f"1r1c{name}",
            df,
            {"sheet_name": name},
        )
        for name, df in atomics.items()
        # single empty field not working
        if not (df.size == 1 and pd.isna(df.iloc[0, 0]))
    ]

    return test_frames


def get_1r1c_tests_sql(atomics: dict):
    test_frames = [
        _Test(
            f"1r1c{name}",
            df,
            {},
        )
        for name, df in atomics.items()
        # single empty field not working
        # if not (df.size == 1 and pd.isna(df.iloc[0, 0]))
    ]

    return test_frames


def id_func(testcase_vals):
    return "_".join(
        (
            f"{name if not (name == 'name' or isinstance(value, dict)) else ''}"
            f"{value if not isinstance(value, dict) else '_'.join((f'{k}{v}') for k, v in value.items())}"  # noqa
        )
        for name, value in testcase_vals._asdict().items()
        if not isinstance(value, pd.DataFrame)
    )


def round_trip_file(test_case: _Test, request, test_type: str, query: str = None):
    # Access the fields of the Test named tuple using dot notation
    test_name = request.node.callspec.id
    df = test_case.df
    kwargs = test_case.kwargs

    if test_type == "xlsx" or test_type == "csv":
        test_url = test_name + "." + test_type
    elif test_type in ("mssql", "mssql+pymssql", "mssql+pyodbc"):
        db_host = os.getenv("TEST_ELS_MSSQL_HOST", "localhost")
        test_url = f"{test_type}://sa:dbatools.I0@{db_host}/els"
        if query:
            test_url += f"?{query}"
    elif test_type == "sqlite":
        test_url = "sqlite:///test_lite.db"
    elif test_type == "duckdb":
        test_url = "duckdb:///test_duck.db"

    outbound_config = Config()
    outbound_config.target.url = test_url
    if test_type == "xlsx":
        outbound_config.target.table = kwargs["sheet_name"]
    if outbound_config.target.type_is_db:
        outbound_config.target.if_exists = "replace"
    outbound_config.source.url = el.urlize_dict({test_name: df})

    # ensuring there is only one staged_frame allows for an implicit pickup of table name
    # note above and line below negates this requirement: t_config.source.table = test_name

    execute(outbound_config)

    inbound_config = get_df_config(df)
    inbound_config.source.url = test_url
    inbound = {}
    inbound_config.target.url = el.urlize_dict(inbound)
    if test_type == "xlsx":
        inbound_config.source.table = kwargs["sheet_name"]
    if inbound_config.source.type_is_db:
        inbound_config.source.table = test_name

    test_els = "__.els.yml"
    yaml.dump(
        inbound_config.model_dump(exclude_none=True),
        open(test_els, "w"),
        sort_keys=False,
        allow_unicode=True,
    )

    execute(test_els)

    if test_type == "xlsx":
        df2 = inbound[kwargs["sheet_name"]]
    else:
        df2 = inbound[test_name]
    assert df.dtypes.equals(df2.dtypes)

    for col in df.columns:
        for row in df[col].index:
            if isinstance(df.at[row, col], type(pd.NA)):
                assert isinstance(df2.at[row, col], type(pd.NA))
            elif isinstance(df.at[row, col], type(pd.NaT)):
                assert isinstance(df2.at[row, col], type(pd.NaT))
            elif isinstance(df.at[row, col], str):
                # calamine reader reads line breaks with carriage return when tox testing
                assert df.at[row, col] == df2.at[row, col].replace("\r\n", "\n")
                # assert df.at[row, col] == df2.at[row, col]
            else:
                assert df.at[row, col] == df2.at[row, col]


def create_test_class_file(
    get_frames_func, test_name, get_tests_func, extension, query
):
    def get_tests():
        atomic_frames = get_frames_func()
        return get_tests_func(atomic_frames)

    class IoTemplate:
        @pytest.mark.parametrize("test_case", get_tests(), ids=id_func)
        def test_round_trip(
            self,
            test_case: _Test,
            request,
            tmp_path,
        ):
            os.chdir(tmp_path)
            round_trip_file(test_case, request, extension, query)

    IoTemplate.__name__ = test_name
    return IoTemplate


class TestCSV:
    pass


class TestExcel:
    pass


class TestMSSQL:
    pass


class TestMSSQL_TDS:
    pass


class TestMSSQL_ODBC17:
    pass


class TestMSSQL_ODBC18:
    pass


class TestSQLite:
    pass


class TestDuckDb:
    pass


test_classes = {
    "TestString": get_atomic_string_frames,
    "TestNumber": get_atomic_number_frames,
    "TestTime": get_atomic_time_frames,
    "TestFaker": get_faker_frames,
    # bools are rare in datasets + pandas has a bug with Bool64
    # "TestBool": get_atomic_bool_frames,
}


for testset in (
    (TestExcel, get_1r1c_tests_excel, "xlsx", None),
    (TestCSV, get_1r1c_tests_csv, "csv", None),
    (TestMSSQL, get_1r1c_tests_sql, "mssql", None),
    (TestMSSQL_TDS, get_1r1c_tests_sql, "mssql+pymssql", None),
    (
        TestMSSQL_ODBC17,
        get_1r1c_tests_sql,
        "mssql+pyodbc",
        "driver=odbc driver 17 for sql server",
    ),
    (
        TestMSSQL_ODBC18,
        get_1r1c_tests_sql,
        "mssql+pyodbc",
        "driver=odbc driver 18 for sql server&TrustServerCertificate=yes",
    ),
    (TestSQLite, get_1r1c_tests_sql, "sqlite", None),
    (TestDuckDb, get_1r1c_tests_sql, "duckdb", None),
):
    for class_name, get_frames_func in test_classes.items():
        setattr(
            testset[0],
            class_name,
            create_test_class_file(
                get_frames_func, class_name, testset[1], testset[2], testset[3]
            ),
        )
