# def test_els_tree(tmp_path):
#     subprocess.run(["pwsh", "D:\\Sync\\repos\\els\\tests\\docs\\generate_example.ps1"])

#     result = subprocess.run(
#         ["git", "diff", "--name-only", "."],
#         capture_output=True,
#         text=True,
#         cwd="D:\\Sync\\repos\\els\\tests\\docs\\controls",
#     )
#     assert result.stdout == ""


# def test_els_tree_lite(tmp_path):
#     subprocess.run(
#         ["pwsh", "D:\\Sync\\repos\\els\\tests\\docs\\generate_example_lite.ps1"]
#     )

#     result = subprocess.run(
#         ["git", "diff", "--name-only", "."],
#         capture_output=True,
#         text=True,
#         cwd="D:\\Sync\\repos\\els\\tests\\docs\\controls_lite",
#     )
#     assert result.stdout == ""
