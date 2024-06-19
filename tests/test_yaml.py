import pandas as pd
import collections
import pytest

import yaml

# import sqlite3

import os
import glob

from eel.cli import execute
from eel.execute import staged_frames
from eel.path import get_config_default

import logging


Test = collections.namedtuple("Test", ["name", "df", "kwargs"])


# def start_logging():
# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a file handler
handler = logging.FileHandler("d:\\Sync\\repos\\eel\\temp\\running_log.log")
handler.setLevel(logging.INFO)

# Create a logging format
formatter = logging.Formatter(
    "\t\t\t\t\t\t\t\t\t%(asctime)s - %(name)s - %(levelname)s:\n%(message)s"
)
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)


@pytest.fixture(autouse=True, scope="session")
def setup():
    os.chdir(os.path.join(".", "temp"))
    logger.info("Getting Started")

    # remove files in the temp directory
    temp_files = glob.glob("*.*")
    for file in temp_files:
        if not file.endswith(".log"):
            os.remove(file)
    yield


def test_cwd():
    assert "d:\\Sync\\repos\\eel\\temp" == os.getcwd()


# Get the ec.Config dictionary for a given DataFrame
def get_df_config(df: pd.DataFrame) -> dict:
    config_df = get_config_default()
    config_df.source.dtype = df.dtypes.apply(lambda x: x.name).to_dict()
    config_dict = config_df.model_dump(exclude_none=True)
    return config_dict


def test_enum_conversion():
    config = get_config_default()
    assert config.target.consistency == "strict"
    assert config.target.if_exists == "fail"


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
    assert type(pd.array([py_val]).dtype) == dtype


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
        Test(
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
        Test(
            f"1r1c{name}",
            df,
            {"sheet_name": name},
        )
        for name, df in atomics.items()
        # single empty field not working
        if not (df.size == 1 and pd.isna(df.iloc[0, 0]))
    ]

    return test_frames


def id_func(testcase_vals):
    return "_".join(
        (
            f"{name if not (name == 'name' or isinstance(value,dict) ) else ''}"
            f"{value if not isinstance(value,dict) else '_'.join( (f'{k}{v}') for k,v in value.items())}"  # noqa
        )
        for name, value in testcase_vals._asdict().items()
        if not isinstance(value, pd.DataFrame)
    )


# def get_target_config():
#     target = ec.Target()


# def add_pandas_end_point(type, direction, df, **kwargs):
#     pandas_end_points[(type, direction)] = df

# if extension == "csv":
#     pandas_end_points[test_name] = df
# elif extension == "xlsx":
#     pandas_end_points[kwargs["sheet_name"]] = df


def round_trip_file(test_case: Test, request, extension: str):
    # Access the fields of the Test named tuple using dot notation
    test_name = request.node.callspec.id
    df = test_case.df
    kwargs = test_case.kwargs
    test_file = test_name + "." + extension

    t_config = get_config_default()
    t_config.source.type = "pandas"
    # t_config.target.file_path = test_name + "." + extension
    t_config.target.type = "." + extension
    t_config.target.file_path = test_file
    if extension == "xlsx":
        t_config.target.table = kwargs["sheet_name"]
    t_config.source.table = test_name

    # t_config.target.table = str(t_config.pipe_id)
    test_eel_out = test_file + ".out.eel.yml"

    staged_frames[test_name] = df

    yaml.dump(
        t_config.model_dump(exclude_none=True),
        open(test_eel_out, "w"),
        sort_keys=False,
        allow_unicode=True,
    )

    execute(test_eel_out)

    # to_func = getattr(df, to_func_name)
    # to_func(test_file, index=False, **kwargs)

    df_config = get_df_config(df)
    if extension == "xlsx":
        df_config["source"]["table"] = kwargs["sheet_name"]
    test_eel = test_file + ".eel.yml"
    yaml.dump(
        df_config,
        open(test_eel, "w"),
        sort_keys=False,
        allow_unicode=True,
    )

    staged_frames.clear()

    execute(test_file)
    logger.info(test_name)

    logger.info(df.dtypes)
    logger.info(df)

    if extension == "csv":
        df2 = staged_frames[test_name]
    elif extension == "xlsx":
        df2 = staged_frames[kwargs["sheet_name"]]
        logger.info(kwargs["sheet_name"])

    assert True

    # logger.info(df2.dtypes)
    # logger.info(df2)

    # assert df.equals(df2)

    # os.remove(test_eel)
    # os.remove(test_eel_out)
    # os.remove(test_file)


# def round_trip_db(test_case: Test, request, table_name):
#     # Access the fields of the Test named tuple using dot notation
#     test_name = request.node.callspec.id
#     df = test_case.df
#     kwargs = test_case.kwargs

#     # Create a SQLite database in memory
#     conn = sqlite3.connect(":memory:")

#     # Write the DataFrame to the SQLite database
#     df.to_sql(table_name, conn, if_exists="replace", index=False, **kwargs)

#     # Read the table back into a DataFrame
#     df2 = pd.read_sql_table(table_name, conn)

#     # Close the SQLite database
#     conn.close()

#     # Rest of your code...


def create_test_class_file(get_frames_func, test_name, get_tests_func, extension):
    def get_tests():
        atomic_frames = get_frames_func()
        return get_tests_func(atomic_frames)

    class IoTemplate:
        @pytest.mark.parametrize("test_case", get_tests(), ids=id_func)
        def test_round_trip(self, test_case: Test, request):
            round_trip_file(test_case, request, extension)

    IoTemplate.__name__ = test_name
    return IoTemplate


class TestCSV:
    pass


class TestExcel:
    pass


test_classes = {
    "TestString": get_atomic_string_frames,
    "TestNumber": get_atomic_number_frames,
    # bools are rare in datasets + pandas has a bug with them
    # "TestBool": get_atomic_bool_frames,
}


# def create_test_class_db(atomic_func, test_name, get_tests_func, table_name):
#     def get_tests():
#         atomic_results = atomic_func()
#         return get_tests_func(atomic_results)

#     class SqliteTemplate:
#         @pytest.mark.parametrize("test_case", get_tests(), ids=id_func)
#         def test_round_trip(self, test_case: Test, request):
#             round_trip_db(test_case, request, table_name)

#     SqliteTemplate.__name__ = test_name
#     return SqliteTemplate


# class TestSQLite:
#     pass


for class_name, get_frames_func in test_classes.items():
    setattr(
        TestCSV,
        class_name,
        create_test_class_file(get_frames_func, class_name, get_1r1c_tests_csv, "csv"),
    )

    setattr(
        TestExcel,
        class_name,
        create_test_class_file(
            get_frames_func, class_name, get_1r1c_tests_excel, "xlsx"
        ),
    )
    # setattr(TestMssql, class_name, create_test_class_mssql(func, class_name))
