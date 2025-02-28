import io
import os

staged_frames = {}
open_files = {}
write_files = set()


def fetch_file_io(url: str, replace_file: bool = False):
    if url is None:
        raise Exception("Cannot fetch None url")
    elif replace_file:
        res = io.BytesIO()
    elif url in open_files:
        res = open_files[url]
    elif os.path.isfile(url):
        with open(url, "rb") as file:
            res = io.BytesIO(file.read())
    else:
        res = io.BytesIO()
    open_files[url] = res
    return res
