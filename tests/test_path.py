import os
from eel.path import CAP2
from eel.path import ContentAwarePath
from pathlib import Path

def test_path():
    p = Path()
    assert str(p.absolute()) == os.getcwd()
    assert type(p) == type(Path())
    
def test_capath1():
    p = ContentAwarePath()
    assert str(p.absolute()) == os.getcwd()
    assert type(p) == type(ContentAwarePath())

def test_capath2():
    p = CAP2()
    assert str(p.absolute()) == os.getcwd()
    assert type(p) == type(CAP2())
    