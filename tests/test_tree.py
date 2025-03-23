import os
import sys
import tempfile
import re
from pathlib import Path

import pytest

from els.cli import tree


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
    └── {dummyroot} → dict://[0-9]+#{target_table}
"""
            else:
                expected = f"""{dummyfile}
└── {dummyroot} → dict://[0-9]+#{target_table}
"""
        else:
            if source_config or keep_virtual:
                expected = f"""{configdir}
└── {dummyfile}.els.yml
    └── {dummyfile}
        └── {dummyroot} → dict://[0-9]+#{target_table}
"""
            else:
                expected = f"""{configdir}
└── {dummyfile}
    └── {dummyroot} → dict://[0-9]+#{target_table}
"""
        # dict://[0-9]+#
        actual = capsys.readouterr().out

        # change out of temp dir so that it can be deleted
        os.chdir("/")
        match = re.match(expected,actual)
        if not match:
            print(f"Actual:\n{actual}")
            print(f"Expected:\n{expected}")
        assert match
    # print("done")
