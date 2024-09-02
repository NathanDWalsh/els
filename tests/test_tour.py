import subprocess


def test_eel_tree(tmp_path, capsys):
    subprocess.run(["pwsh", "D:\\Sync\\repos\\eel\\tests\\docs\\generate_example.ps1"])

    result = subprocess.run(
        ["git", "diff", "--name-only", "."],
        capture_output=True,
        text=True,
        cwd="D:\\Sync\\repos\\eel\\tests\\docs\\controls",
    )
    assert result.stdout == ""
