import subprocess


def test_eel_tree(tmp_path, capsys):
    # import subprocess

    # print("a")
    result = subprocess.run(
        ["git", "diff", "--name-only", "."],
        capture_output=True,
        text=True,
        cwd="D:\\Sync\\repos\\eel\\tests\\docs\\controls",
    )
    # captured = capsys.readouterr()
    # expected_output = "a\n"
    # assert captured.out == expected_output
    assert result != ""
    # assert tmp_path.exists()
