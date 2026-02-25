import json
import os
import socket
import shutil
import sys
from pathlib import Path
from typing import Any


PROJECT_DIR = Path(__file__).resolve().parent.parent
SOURCE_DIR = PROJECT_DIR / "source"
BUILD_DIR = PROJECT_DIR / "build"
DIST_DIR = PROJECT_DIR / "dist"
DEVCONTAINER_PATH = PROJECT_DIR / ".devcontainer" / "devcontainer.json"
VSCODE_SETTINGS_PATH = PROJECT_DIR / ".vscode" / "settings.json"
PWD_PLACEHOLDER = "${containerWorkspaceFolder}"


def _replace_placeholder(value: Any) -> Any:
    if isinstance(value, str):
        return value.replace(PWD_PLACEHOLDER, str(PROJECT_DIR))
    if isinstance(value, list):
        return [_replace_placeholder(item) for item in value]
    if isinstance(value, dict):
        return {key: _replace_placeholder(item) for key, item in value.items()}
    return value


def _run_with_argv(entrypoint, argv: list[str]) -> int:
    original_argv = sys.argv[:]
    try:
        sys.argv = argv
        return int(entrypoint())
    finally:
        sys.argv = original_argv


def _missing_dependency(command_name: str, module_name: str) -> int:
    print(
        f"ERROR: Cannot run '{command_name}' because '{module_name}' is not installed in the Poetry environment."
    )
    print("Run 'poetry install' and try again.")
    return 1


def _remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
        print(f"Removed directory: {path}")
        return
    if path.is_file():
        path.unlink()
        print(f"Removed file: {path}")
        return
    print(f"Path does not exist: {path}")


def _can_bind(host: str, port: int) -> tuple[bool, str]:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((host, port))
    except OSError as error:
        return False, str(error)
    return True, ""


def settings() -> int:
    if not DEVCONTAINER_PATH.exists():
        print(f"ERROR: Missing devcontainer file: {DEVCONTAINER_PATH}")
        return 1

    with DEVCONTAINER_PATH.open("r", encoding="utf-8") as handle:
        config = json.load(handle)

    try:
        vscode_settings = config["customizations"]["vscode"]["settings"]
    except KeyError:
        print("ERROR: '.devcontainer/devcontainer.json' is missing customizations.vscode.settings")
        return 1

    resolved_settings = _replace_placeholder(vscode_settings)

    VSCODE_SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with VSCODE_SETTINGS_PATH.open("w", encoding="utf-8") as handle:
        json.dump(resolved_settings, handle, indent=4)
        handle.write("\n")

    print(f"Updated VS Code settings: {VSCODE_SETTINGS_PATH}")
    return 0


def build() -> int:
    if not SOURCE_DIR.exists():
        print(f"ERROR: Missing Sphinx source directory: {SOURCE_DIR}")
        return 1

    try:
        from sphinx.cmd.build import main as sphinx_build_main
    except ModuleNotFoundError:
        return _missing_dependency("build", "sphinx")

    argv = ["sphinx-build", "-M", "html", str(SOURCE_DIR), str(BUILD_DIR)]
    return _run_with_argv(sphinx_build_main, argv)


def esbonio() -> int:
    if not SOURCE_DIR.exists():
        print(f"ERROR: Missing Sphinx source directory: {SOURCE_DIR}")
        return 1

    try:
        from sphinx.cmd.build import main as sphinx_build_main
    except ModuleNotFoundError:
        return _missing_dependency("esbonio", "sphinx")

    argv = ["sphinx-build", "-M", "html", str(SOURCE_DIR), str(BUILD_DIR / "esbonio")]
    return _run_with_argv(sphinx_build_main, argv)


def auto() -> int:
    if not SOURCE_DIR.exists():
        print(f"ERROR: Missing Sphinx source directory: {SOURCE_DIR}")
        return 1

    try:
        from sphinx_autobuild.__main__ import main as sphinx_autobuild_main
    except ModuleNotFoundError:
        return _missing_dependency("auto", "sphinx-autobuild")

    host = os.environ.get("SPHINX_AUTOBUILD_HOST", "127.0.0.1")
    try:
        port = int(os.environ.get("SPHINX_AUTOBUILD_PORT", "8000"))
    except ValueError:
        print("ERROR: SPHINX_AUTOBUILD_PORT must be an integer.")
        return 1

    can_bind, bind_error = _can_bind(host, port)
    if not can_bind:
        print(f"ERROR: Cannot bind sphinx-autobuild server to {host}:{port} ({bind_error})")
        print("Set SPHINX_AUTOBUILD_HOST/SPHINX_AUTOBUILD_PORT to an allowed interface and port.")
        return 1

    argv = [
        "sphinx-autobuild",
        "--host",
        host,
        "--port",
        port,
        str(SOURCE_DIR),
        str(BUILD_DIR / "html"),
    ]
    return _run_with_argv(sphinx_autobuild_main, argv)


def clean(path: str = "") -> int:
    clean_target_argument = path
    cli_arguments = [argument for argument in sys.argv[1:] if argument != "--"]
    if not clean_target_argument and cli_arguments:
        clean_target_argument = cli_arguments[0]

    if clean_target_argument:
        clean_target = Path(clean_target_argument)
        if not clean_target.is_absolute():
            clean_target = PROJECT_DIR / clean_target
        _remove_path(clean_target)
        return 0

    _remove_path(BUILD_DIR)
    _remove_path(DIST_DIR)
    return 0
