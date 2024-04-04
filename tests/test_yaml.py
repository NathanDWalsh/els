import pandas as pd
import collections
import pytest
import yaml
import numpy as np

import os
import glob

import eel.config as ec

from eel.cli import execute
from eel.execute import pandas_end_points
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
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)

logger.info("Getting Started")


@pytest.fixture(autouse=True, scope="session")
def setup():
    os.chdir(os.path.join(".", "temp"))
    # start_logging()
    temp_files = glob.glob("*.*")
    for file in temp_files:
        if not file.endswith(".log"):
            os.remove(file)
    yield


def test_cwd():
    assert "d:\\Sync\\repos\\eel\\temp" == os.getcwd()


# TODO: move this to eel.config?
def build_default_config() -> ec.Config:
    return ec.Config()


def is_numeric(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


# Get the ec.Config dictionary for a given DataFrame
def get_df_config(df: pd.DataFrame) -> dict:
    config_df = get_config_default()
    # if config_df.source.read_csv is None:
    #     config_df.source.read_csv = ec.ReadCsv()
    # config_df.source.read_csv.dtype = df.dtypes.apply(lambda x: x.name).to_dict()
    config_df.source.dtype = df.dtypes.apply(lambda x: x.name).to_dict()

    # this is to prevent empty strings from being read as NaN:
    # config_df.source.read_csv.keep_default_na = False
    # config_df.source.read_csv.na_values = ["nan"]
    # = ["NaN", "Null", "null", "#NA", "N/A", "#N/A"]

    # config_df.source.read_csv.na_values = None

    config_dict = config_df.model_dump(exclude_none=True)

    return config_dict


def test_enum_conversion():
    config = get_config_default()
    assert config.target.consistency == "strict"
    assert config.target.if_exists == "fail"


# # Test python type to numpy type conversion and euqality
# @pytest.mark.parametrize(
#     "py_val, dtypes",
#     [
#         (1, (int, np.int32)),
#         # (1.1, (float, np.float64)),
#         # (True, (bool, np.bool_)),
#         # ("a", (str, np.str_)),
#     ],
# )
# def test_np_type_equality(py_val, dtypes):
#     # numpy type should be the same as the python type
#     assert np.dtype(dtypes[0]) == dtypes[1]
#     for dtype in dtypes:
#         # numpy type should be the same as the inferred python type
#         assert np.array(py_val).dtype.type == np.dtype(dtype)


# Test python type to numpy type conversion and euqality
@pytest.mark.parametrize(
    "py_val, dtype",
    [
        (1, pd.Int64Dtype),
        (1.1, pd.Float64Dtype),
        (True, pd.BooleanDtype),
        ("a", pd.StringDtype),
    ],
)
def test_np_type_equality(py_val, dtype):
    # numpy type should be the same as the python type
    assert type(pd.array([py_val]).dtype) == dtype


def get_atomic_strings():
    res = {
        "Stringy": "a",
        "Inty": "1",
        "Floaty": "1.1",
        "Booly": "True",
        "Datey": "2021-01-01",
        "Datetimey": "2021-01-01 00:00:00",
        "Null": None,
        # These will always be imported as NaN
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


def get_atomic_number_arrays():
    res = {
        # f"{num_type.__name__.capitalize()}({num_val})": np.array(num_val)
        f"{num_type.name.capitalize()}({num_val})": pd.array(
            [num_val], dtype=num_type.name
        )
        # .astype(np.dtype(num_type))
        # .item()
        # for num_type in [int, np.int32, np.int64, bool, np.bool_, float, np.float64]
        for num_type in [pd.Float64Dtype, pd.Int64Dtype]
        for num_val in [-1, 0, 1, None]
    }

    return res


# def get_atomic_nans():
#     atomic_NaNs = {
#         f"NaN({num_type.__name__.capitalize()})": np.array(np.nan)
#         .astype(np.dtype(num_type))
#         .item()
#         # for num_type in [int, np.int32, np.int64, bool, np.bool_, float, np.float64]
#         for num_type in [float, np.float64]
#     }
#     return atomic_NaNs


def get_atomic_str_nans():
    atomic_NaNs = {"NaN(Object)": np.array(np.nan).astype("object").item()}

    return atomic_NaNs


def get_1r1c_tests(atomics: dict):

    test_frames = [
        Test(
            f"1r1c{name}",
            df,
            {"quoting": quoting},
        )
        for name, df in atomics.items()
        # for quoting in [0, 1, 2, 3]
        for quoting in [0]
        # TODO: consider adding 4 and 5 for later versions
        # if not (
        #     quoting == 3
        #     # and (val in ("", None))
        #     and (
        #         df[0]
        #         in (pd.NA)
        #         # or ((not isinstance(pd_arr, str)) and (np.isnan(pd_arr)))
        #     )  # or (np.isnan(val)))
        #     # or ((not isinstance(val, str)) and np.isnan(val))
        # )
    ]

    return test_frames


# def get_1r1c_tests(atomics: dict):

#     test_frames = [
#         Test(
#             f"1r1c{type(val).__name__.capitalize()}{name}",
#             pd.DataFrame(
#                 {name: [val]},
#                 dtype="string" if "Object" in name or "Str" in name else None,
#             ),
#             {"quoting": quoting},
#         )
#         for name, val in atomics.items()
#         # for quoting in [0, 1, 2, 3]
#         for quoting in [0]
#         # consider adding 4 and 5 for later versions
#         if not (
#             quoting == 3
#             # and (val in ("", None))
#             and (
#                 val in ("", None) or ((not isinstance(val, str)) and (np.isnan(val)))
#             )  # or (np.isnan(val)))
#             # or ((not isinstance(val, str)) and np.isnan(val))
#         )
#     ]

#     return test_frames


def id_func(testcase_vals):
    # if isinstance(testcase_vals, Test):
    return "_".join(
        (
            f"{name if not (name == 'name' or isinstance(value,dict) ) else ''}"
            f"{value if not isinstance(value,dict) else '_'.join( (f'{k}{v}') for k,v in value.items())}"  # noqa
        )
        for name, value in testcase_vals._asdict().items()
        if not isinstance(value, pd.DataFrame)
    )
    # return ""


def round_trip_csv(test_case: Test, request):
    # Access the fields of the Test named tuple using dot notation
    test_name = request.node.callspec.id
    df = test_case.df
    kwargs = test_case.kwargs

    test_file = test_name + ".csv"
    df.to_csv(test_file, index=False, **kwargs)

    df_config = get_df_config(df)
    test_eel = test_file + ".eel.yml"
    yaml.dump(
        df_config,
        open(test_eel, "w"),
        sort_keys=False,
        allow_unicode=True,
    )
    execute(test_file)
    logger.info(test_name)
    df2 = pandas_end_points[test_name]

    logger.info(df.dtypes)
    logger.info(df2.dtypes)

    assert df.equals(df2)

    # os.remove(test_eel)
    # os.remove(test_file)


def round_trip_excel(test_name: str, df: pd.DataFrame):
    df.to_excel("test.xlsx", index=False)
    df2 = pd.read_excel("test.xlsx")
    assert df.equals(df2)
    # os.remove("test.xlsx")


def create_test_class_csv(atomic_func, test_name):
    def get_tests():
        atomic_results = atomic_func()
        return get_1r1c_tests(atomic_results)

    class CsvTemplate:
        @pytest.mark.parametrize("test_case", get_tests(), ids=id_func)
        def test_csv(self, test_case: Test, request):
            round_trip_csv(test_case, request)

    CsvTemplate.__name__ = test_name
    return CsvTemplate


def create_test_class_excel(atomic_func, test_name):
    def get_tests():
        atomic_results = atomic_func()
        return get_1r1c_tests(atomic_results)

    class CsvTemplate:
        @pytest.mark.parametrize("test_case", get_tests(), ids=id_func)
        def test_excel(self, test_case: Test, request):
            round_trip_excel(test_case, request)

    CsvTemplate.__name__ = test_name
    return CsvTemplate


class TestCSV:
    pass


class TestExcel:
    pass


test_classes = {
    "TestString": get_atomic_string_frames,
    # "TestNumber": get_atomic_number_arrays,
    # "TestStrNan": get_atomic_str_nans,
    # "TestNan": get_atomic_nans,
}

for class_name, func in test_classes.items():
    setattr(TestCSV, class_name, create_test_class_csv(func, class_name))
    # setattr(TestExcel, class_name, create_test_class_excel(func, class_name))
