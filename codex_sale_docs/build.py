import shutil
import sys

from sphinx.cmd.build import main as sphinx_build_main
from sphinx_autobuild.__main__ import main as sphinx_autobuild_main


SOURCE_DIR = "source"
BUILD_DIR = "build"


def build() -> int:
    sys.argv = ["sphinx-build", "-M", "html", SOURCE_DIR, BUILD_DIR]
    return sphinx_build_main()


def auto() -> int:
    sys.argv = ["sphinx-autobuild", "--host", "0.0.0.0", SOURCE_DIR, f"{BUILD_DIR}/html"]
    return sphinx_autobuild_main()


def clean(path: str = BUILD_DIR) -> int:
    try:
        shutil.rmtree(path)
        print(f"Removed directory: {path}")
    except FileNotFoundError:
        print(f"Directory does not exist: {path}")
    return 0
