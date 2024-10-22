import os
import sys
import tempfile
from pathlib import Path

import pytest

from els.cli import tree

# @pytest.mark.parametrize(
#     "py_val, dtype",
#     [
#         (1, pd.Int64Dtype),
#         (1.1, pd.Float64Dtype),
#         (True, pd.BooleanDtype),
#         ("a", pd.StringDtype),
#     ],
# )
# def test_pd_type_equality(py_val, dtype):


@pytest.mark.parametrize("explicit_context", [True, False])
@pytest.mark.parametrize("pass_directory", [True, False])
@pytest.mark.parametrize("root_config", [True, False])
@pytest.mark.parametrize("dir_config", [True, False])
@pytest.mark.parametrize("source_config", [True, False])
@pytest.mark.parametrize("keep_virtual", [True, False])
def test_tree(
    explicit_context,
    pass_directory,
    root_config,
    dir_config,
    source_config,
    keep_virtual,
    capsys,
    tmp_path,
):
    configdir = "config"
    dummyfile = "dummy.csv"
    dummyroot = dummyfile.split(".")[0]

    default_table = dummyroot
    root_table = "roottab"
    dir_table = "dirtab"
    source_table = "sourcetab"

    target_table = (
        source_table
        if source_config
        else dir_table
        if dir_config
        else root_table
        if root_config
        else default_table
    )

    kwargs = {}
    # kwargs["dir"] = Path("D:\\Sync\\repos") / "temp"
    kwargs["dir"] = tmp_path
    if sys.version_info < (3, 12):
        pass
    else:
        kwargs["delete"] = False

    with tempfile.TemporaryDirectory(**kwargs) as tmpdirname:
        os.chdir(tmpdirname)
        # create a dummy csv file
        os.mkdir(configdir)
        os.chdir(configdir)
        with open(dummyfile, "w") as file:
            file.write("a,b,c\n1,2,3\n4,5,6\n")

        if root_config:
            with open("__.els.yml", "w") as file:
                file.write(f"target:\n  table: {root_table}")
        if dir_config:
            with open("_.els.yml", "w") as file:
                file.write(f"target:\n  table: {dir_table}")
        if source_config:
            with open(f"{dummyfile}.els.yml", "w") as file:
                file.write(f"target:\n  table: {source_table}")

        # run the tree command and capture the output
        if explicit_context:
            if pass_directory:
                tree(str(Path(tmpdirname) / configdir), keep_virtual)
            else:
                tree(str(Path(tmpdirname) / configdir / dummyfile), keep_virtual)
        else:
            tree(keep_virtual=keep_virtual)

        if (
            explicit_context
            and not pass_directory
            and not root_config
            and not dir_config
        ):
            if source_config or keep_virtual:
                expected = f"""{dummyfile}.els.yml
└── {dummyfile}
    └── {dummyroot} → memory['{target_table}']
"""
            else:
                expected = f"""{dummyfile}
└── {dummyroot} → memory['{target_table}']
"""
        else:
            if source_config or keep_virtual:
                expected = f"""{configdir}
└── {dummyfile}.els.yml
    └── {dummyfile}
        └── {dummyroot} → memory['{target_table}']
"""
            else:
                expected = f"""{configdir}
└── {dummyfile}
    └── {dummyroot} → memory['{target_table}']
"""

        actual = capsys.readouterr().out

        # change out of temp dir so that it can be deleted
        os.chdir("/")
        assert actual == expected
    # print("done")


# Create a pytest fixture for capsys
@pytest.fixture
def capsys_fixture():
    return pytest.capsys


# # Directly call the test function
# def main():
#     # capsys = capsys_fixture()
#     test_tree(
#         explicit_context=False,
#         pass_directory=True,
#         root_config=True,
#         dir_config=True,
#         source_config=False,
#         keep_virtual=True,
#         capsys=capsys,
#     )
#     # print(capsys.readouterr().out)


# if __name__ == "__main__":
# main()
