"""Poetry command dispatchers rooted in the workflow package layout."""

from __future__ import annotations

import importlib
import json
import os
import shutil
import socket
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent
SOURCE_DIR = PROJECT_ROOT / "source"
BUILD_DIR = PROJECT_ROOT / "build"
DIST_DIR = PROJECT_ROOT / "dist"
DEVCONTAINER_PATH = PROJECT_ROOT / ".devcontainer" / "devcontainer.json"
VSCODE_SETTINGS_PATH = PROJECT_ROOT / ".vscode" / "settings.json"
PWD_PLACEHOLDER = "${containerWorkspaceFolder}"

if str(SOURCE_DIR) not in sys.path:
    sys.path.insert(0, str(SOURCE_DIR))


def _dispatch(module_path: str, function_name: str = "main", *args, **kwargs) -> int:
    module = importlib.import_module(module_path)
    function = getattr(module, function_name)
    return int(function(*args, **kwargs))


def _replace_placeholder(value: Any) -> Any:
    if isinstance(value, str):
        return value.replace(PWD_PLACEHOLDER, str(PROJECT_ROOT))
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
    print(f"ERROR: Cannot run '{command_name}' because '{module_name}' is not installed in the Poetry environment.")
    print("Run 'poetry install' and try again.")
    return 1


def _prepare_generated_docs() -> int:
    result = _dispatch("generate_workflow_diagram", "run_default")
    if result != 0:
        return result
    return 0


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


def settings(*args, **kwargs) -> int:
    return _dispatch("city.fetch_subject_street_assessments.get_city_data", "main", *args, **kwargs)


def generate_workflow_diagram(*args, **kwargs) -> int:
    return _dispatch("generate_workflow_diagram", "main", *args, **kwargs)


def workspace_settings(*args, **kwargs) -> int:
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


def build(*args, **kwargs) -> int:
    if not SOURCE_DIR.exists():
        print(f"ERROR: Missing Sphinx source directory: {SOURCE_DIR}")
        return 1

    prep_result = _prepare_generated_docs()
    if prep_result != 0:
        return prep_result

    try:
        from sphinx.cmd.build import main as sphinx_build_main
    except ModuleNotFoundError:
        return _missing_dependency("build", "sphinx")

    argv = ["sphinx-build", "-M", "html", str(SOURCE_DIR), str(BUILD_DIR)]
    return _run_with_argv(sphinx_build_main, argv)


def esbonio(*args, **kwargs) -> int:
    if not SOURCE_DIR.exists():
        print(f"ERROR: Missing Sphinx source directory: {SOURCE_DIR}")
        return 1

    prep_result = _prepare_generated_docs()
    if prep_result != 0:
        return prep_result

    try:
        from sphinx.cmd.build import main as sphinx_build_main
    except ModuleNotFoundError:
        return _missing_dependency("esbonio", "sphinx")

    argv = ["sphinx-build", "-M", "html", str(SOURCE_DIR), str(BUILD_DIR / "esbonio")]
    return _run_with_argv(sphinx_build_main, argv)


def auto(*args, **kwargs) -> int:
    if not SOURCE_DIR.exists():
        print(f"ERROR: Missing Sphinx source directory: {SOURCE_DIR}")
        return 1

    prep_result = _prepare_generated_docs()
    if prep_result != 0:
        return prep_result

    try:
        from sphinx_autobuild.__main__ import main as sphinx_autobuild_main
    except ModuleNotFoundError:
        return _missing_dependency("auto", "sphinx-autobuild")

    host = os.environ.get("SPHINX_AUTOBUILD_HOST", "127.0.0.1")
    try:
        port = int(os.environ.get("SPHINX_AUTOBUILD_PORT", "0"))
    except ValueError:
        print("ERROR: SPHINX_AUTOBUILD_PORT must be an integer.")
        return 1
    require_server = os.environ.get("SPHINX_AUTOBUILD_REQUIRE_SERVER", "").lower() in {
        "1",
        "true",
        "yes",
    }

    can_bind, bind_error = _can_bind(host, port)
    if not can_bind:
        if require_server:
            print(f"ERROR: Cannot bind sphinx-autobuild server to {host}:{port} ({bind_error})")
            print("Set SPHINX_AUTOBUILD_HOST/SPHINX_AUTOBUILD_PORT to an allowed interface and port.")
            return 1
        print(f"WARNING: Cannot bind sphinx-autobuild server to {host}:{port} ({bind_error})")
        print("Falling back to one-shot HTML build. Set SPHINX_AUTOBUILD_REQUIRE_SERVER=1 to force failure.")
        return build()

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
            clean_target = PROJECT_ROOT / clean_target
        _remove_path(clean_target)
        return 0

    _remove_path(BUILD_DIR)
    _remove_path(DIST_DIR)
    return 0


def check_provenance(*args, **kwargs) -> int:
    return _dispatch("fair_market_value.write_fmv_justification.check_provenance", "main", *args, **kwargs)


def fetch_open_calgary(*args, **kwargs) -> int:
    return _dispatch("assessment_comps.match_same_unit_across_buildings.fetch_open_calgary", "main", *args, **kwargs)


def prepare_renter_comps(*args, **kwargs) -> int:
    return _dispatch("assessment_comps.find_same_floor_value_peers.prepare_renter_comps", "main", *args, **kwargs)


def infer_open_calgary_units(*args, **kwargs) -> int:
    return _dispatch("assessment_comps.generalize_cross_building_matches.infer_open_calgary_units", "main", *args, **kwargs)


def fetch_city_data(*args, **kwargs) -> int:
    return _dispatch("city.fetch_subject_street_assessments.fetch_city_data", "main", *args, **kwargs)


def city_data_to_rst(*args, **kwargs) -> int:
    return _dispatch("city.build_building_unit_inventory.city_data_to_rst", "main", *args, **kwargs)


def get_city_data(*args, **kwargs) -> int:
    return _dispatch("city.fetch_subject_street_assessments.get_city_data", "main", *args, **kwargs)


def city_data_enums(*args, **kwargs) -> int:
    return _dispatch("city.build_building_unit_inventory.city_data_enums", "main", *args, **kwargs)


def city_data_inventory(*args, **kwargs) -> int:
    return _dispatch("city.build_subject_unit_profile.city_data_inventory", "main", *args, **kwargs)


def city_data_metadata_rst(*args, **kwargs) -> int:
    return _dispatch("city.build_building_unit_inventory.city_data_metadata_rst", "main", *args, **kwargs)


def audit_realtor_accuracy(*args, **kwargs) -> int:
    return _dispatch("mls_enrichment.cross_validate.audit_realtor_accuracy", "main", *args, **kwargs)


def extract_realtor_listing(*args, **kwargs) -> int:
    return _dispatch("mls_enrichment.fetch_realtor.extract_realtor_listing", "main", *args, **kwargs)


def osm_address_lookup(*args, **kwargs) -> int:
    return _dispatch("mls_enrichment.normalize.osm_address_lookup", "main", *args, **kwargs)


def osm_geocode(*args, **kwargs) -> int:
    return _dispatch("mls_enrichment.normalize.osm_geocode", "main", *args, **kwargs)


def osm_suggest_addresses(*args, **kwargs) -> int:
    return _dispatch("mls_enrichment.normalize.osm_suggest_addresses", "main", *args, **kwargs)


def google_maps_geocode(*args, **kwargs) -> int:
    return _dispatch("mls_enrichment.normalize.google_maps_geocode", "main", *args, **kwargs)


def google_maps_suggest_addresses(*args, **kwargs) -> int:
    return _dispatch("mls_enrichment.normalize.google_maps_suggest_addresses", "main", *args, **kwargs)


def find_realtor_listings_browser(*args, **kwargs) -> int:
    return _dispatch("mls_enrichment.fetch_realtor.find_realtor_listings_browser", "main", *args, **kwargs)
