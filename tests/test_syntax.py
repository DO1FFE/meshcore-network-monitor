from pathlib import Path
import py_compile


BASISPFAD = Path(__file__).resolve().parents[1]


def test_python_dateien_sind_syntaxgueltig() -> None:
    python_dateien = [
        *BASISPFAD.joinpath("scripts").glob("*.py"),
        *BASISPFAD.joinpath("tests").glob("*.py"),
    ]

    assert python_dateien, "Es wurden keine Python-Dateien gefunden."

    for datei in python_dateien:
        py_compile.compile(str(datei), doraise=True)
