import os
from pathlib import Path

import pytest
from typer.testing import CliRunner

from els.cli import app, tree


# @pytest.mark.skip
@pytest.mark.parametrize(
    "cli",
    [
        True,
        # False,
    ],
)
@pytest.mark.parametrize(
    "explicit_context",
    [
        True,
        False,
    ],
)
@pytest.mark.parametrize(
    "pass_directory",
    [
        True,
        False,
    ],
)
@pytest.mark.parametrize(
    "root_config",
    [
        True,
        False,
    ],
)
@pytest.mark.parametrize(
    "dir_config",
    [
        True,
        False,
    ],
)
@pytest.mark.parametrize(
    "source_config",
    [
        True,
        False,
    ],
)
@pytest.mark.parametrize(
    "keep_virtual",
    [
        True,
        False,
    ],
)
@pytest.mark.parametrize(
    "config_dir_deep",
    [
        True,
        False,
    ],
)
def test_tree(
    cli,
    explicit_context,
    pass_directory,
    root_config,
    dir_config,
    source_config,
    keep_virtual,
    capsys,
    tmp_path,
    config_dir_deep,
):
    if config_dir_deep:
        configdir = "deep/config"
    else:
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

    os.chdir(tmp_path)
    if config_dir_deep:
        os.mkdir("deep")
    os.mkdir(configdir)

    # create a dummy csv file
    os.chdir(configdir)
    with open(dummyfile, "w") as file:
        file.write("a,b,c\n1,2,3\n4,5,6\n")

    if root_config:
        with open("__.els.yml", "w") as file:
            file.write(f"target:\n  table: {root_table}\n")
    if dir_config:
        with open("_.els.yml", "w") as file:
            file.write(f"target:\n  table: {dir_table}\n")
    if source_config:
        with open(f"{dummyfile}.els.yml", "w") as file:
            file.write(f"target:\n  table: {source_table}\n")
    if cli:
        runner = CliRunner()
        keep_virtual_cli = "--keep-virtual" if keep_virtual else "--no-keep-virtual"
        if explicit_context:
            os.chdir(tmp_path)
            if pass_directory:
                app_args = [
                    "tree",
                    f"{configdir}",
                    keep_virtual_cli,
                ]
            else:
                app_args = [
                    "tree",
                    f"{str(Path(tmp_path) / configdir / dummyfile)}",
                    keep_virtual_cli,
                ]
        else:
            app_args = [
                "tree",
                keep_virtual_cli,
            ]
        print([str(os.getcwd()), app_args])
        result = runner.invoke(
            app,
            app_args,
        )
        actual = result.stdout
    else:
        # run the tree command and capture the output
        if explicit_context:
            if pass_directory:
                tree(str(Path(tmp_path) / configdir), keep_virtual)
            else:
                tree(str(Path(tmp_path) / configdir / dummyfile), keep_virtual)
        else:
            tree(keep_virtual=keep_virtual)

        actual = capsys.readouterr().out

    if cli:
        target_url = f"stdout://#{target_table}"
    else:
        target_url = f"dict://[0-9]#{target_table}"
    if explicit_context and not pass_directory and not root_config and not dir_config:
        if source_config or keep_virtual:
            expected = f"""{dummyfile}.els.yml
└── {dummyfile}
    └── {dummyroot} → {target_url}
"""
        else:
            expected = f"""{dummyfile}
└── {dummyroot} → {target_url}
"""
    else:
        if not explicit_context:
            configdir = "."
        if source_config or keep_virtual:
            expected = f"""{configdir}
└── {dummyfile}.els.yml
    └── {dummyfile}
        └── {dummyroot} → {target_url}
"""
        else:
            expected = f"""{configdir}
└── {dummyfile}
    └── {dummyroot} → {target_url}
"""

    if not expected == actual:
        print(f"Actual:\n{actual}")
        print(f"Expected:\n{expected}")
    assert expected == actual
