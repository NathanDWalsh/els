def test_eel_tree(tmp_path, capsys):
    # import subprocess

    print("a")
    # result = subprocess.run(["echo", "a"], capture_output=True, text=True)
    captured = capsys.readouterr()
    expected_output = "a\n"
    assert captured.out == expected_output
    assert captured.err == ""
    assert tmp_path.exists()
