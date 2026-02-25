import json
import os
import shutil
import sys


SOURCE_DIR = "source"
BUILD_DIR = "build"
PROJECT_DIR = os.getcwd()
DEVCONTAINER_PATH = os.path.join(PROJECT_DIR, ".devcontainer", "devcontainer.json")
VSCODE_SETTINGS_PATH = os.path.join(PROJECT_DIR, ".vscode", "settings.json")
PWD_PLACEHOLDER = "${containerWorkspaceFolder}"


def settings() -> int:
    with open(DEVCONTAINER_PATH, "r", encoding="utf-8") as handle:
        config = json.load(handle)

    vscode_settings = config["customizations"]["vscode"]["settings"]

    for key, value in list(vscode_settings.items()):
        if isinstance(value, str) and PWD_PLACEHOLDER in value:
            vscode_settings[key] = value.replace(PWD_PLACEHOLDER, PROJECT_DIR)
        elif isinstance(value, list):
            updated_list = []
            for item in value:
                if isinstance(item, str) and PWD_PLACEHOLDER in item:
                    updated_list.append(item.replace(PWD_PLACEHOLDER, PROJECT_DIR))
                else:
                    updated_list.append(item)
            vscode_settings[key] = updated_list

    os.makedirs(os.path.dirname(VSCODE_SETTINGS_PATH), exist_ok=True)
    with open(VSCODE_SETTINGS_PATH, "w", encoding="utf-8") as handle:
        json.dump(vscode_settings, handle, indent=4)
        handle.write("\n")

    print(f"Updated VS Code settings: {VSCODE_SETTINGS_PATH}")
    return 0


def build() -> int:
    from sphinx.cmd.build import main as sphinx_build_main

    sys.argv = ["sphinx-build", "-M", "html", SOURCE_DIR, BUILD_DIR]
    return sphinx_build_main()


def auto() -> int:
    from sphinx_autobuild.__main__ import main as sphinx_autobuild_main

    sys.argv = ["sphinx-autobuild", "--host", "0.0.0.0", SOURCE_DIR, f"{BUILD_DIR}/html"]
    return sphinx_autobuild_main()


def clean(path: str = BUILD_DIR) -> int:
    try:
        shutil.rmtree(path)
        print(f"Removed directory: {path}")
    except FileNotFoundError:
        print(f"Directory does not exist: {path}")
    return 0
