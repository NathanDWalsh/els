import re

import pytest

# import pandas as pd
# from test_yaml import TestCSV


# @pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(items):
    for item in items:
        # use regex to replace item.name with that which is between []
        item.name = re.sub(r".*?(\[.*)", r"\1", item.name)


# def id_func(testcase_vals):
#     # if isinstance(testcase_vals, Test):
#     return "__".join(
#         (
#             f"{name if not (name == 'name' or isinstance(value,dict) ) else ''}"
#             f"{value if not isinstance(value,dict) else '_'.join( (f'{k}{v}') for k,v in value.items())}"  # noqa
#         )
#         for name, value in testcase_vals._asdict().items()
#         if not isinstance(value, pd.DataFrame)
#     )


# def pytest_make_parametrize_id(config, val, argname):
#     if isinstance(val, TestCSV):
#         return id_func(val)
#     else:
#         return None
