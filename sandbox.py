from pathlib import Path
import os

path_ext = Path(".eel.yml")
path_t = Path("asdfdsaf.eel.yml")

print(path_ext.with_name("_" + str(path_ext)).suffixes)
print("".join(path_t.suffixes))

print(path_ext.suffixes == path_t.suffixes)
