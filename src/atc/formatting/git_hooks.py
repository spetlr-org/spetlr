"""
This script installs and runs git commit hooks.
To install the hooks, please run

$> python git_hooks.py

To uninstall the git hooks, run

$> python git_hooks.py uninstall

"""
import subprocess
import sys
from textwrap import dedent
from pathlib import Path
import os


actions = {}


def action(func):
    global actions
    actions[func.__name__.replace("_", "-")] = func


@action
def pre_commit():
    """
    This pre-commit hook runs the black code formatter on all changed files
    that end in ".py"
    """
    print("atc-dataplatform pre-commit hook")

    # this command
    # $> git diff --cached --name-only --diff-filter=d
    # results in an output like
    # src/atc/formatting/git_hooks.py
    # with one line per Modified or New file, deleted files are excluded
    # we don't want to reformat deleted files, all others should be formatted.
    files_to_check = []
    for line in (
        subprocess.run(
            [
                "git",
                "diff",
                "--cached",
                # only show the names, one name per line
                "--name-only",
                # diff filter lowercase d means exclude deleted files
                "--diff-filter=d",
            ],
            capture_output=True,
        )
        .stdout.decode()
        .splitlines()
    ):
        path = line.strip()
        if not path.endswith(".py"):
            continue
        files_to_check.append(path)

    if not files_to_check:
        print("  Nothing to do.")
        return

    # first reformat all affected python files
    subprocess.run(["black", *files_to_check], check=True)
    # now add all reformatted files back to be committed
    subprocess.run(["git", "add", *files_to_check], check=True)


def get_hooks_dir() -> Path:
    for path in [Path.cwd()] + list(Path().cwd().parents):
        if (path / ".git").is_dir():
            break
    else:
        raise AssertionError(
            "Git hooks directory not found. Please run this script form the repo base."
        )

    return path / ".git" / "hooks"


def install() -> None:
    print("Now installing hooks.")

    hooks_dir = get_hooks_dir()

    for command in actions.keys():
        with open(hooks_dir / command, "w") as f:
            print(f"Installing hook for '{command}'")
            f.write(
                dedent(
                    rf"""
                    #!{sys.executable}
                    import sys, subprocess
                    if sys.executable != {repr(sys.executable)}:
                        print("Changing interpreter")
                        subprocess.run([{repr(sys.executable)}, __file__]+sys.argv[1:],check=True)
                        sys.exit(0)

                    try:
                        from atc.formatting.git_hooks import actions
                        actions[{repr(command)}]()
                    except ModuleNotFoundError as e:
                        import sys
                        print(e)
                        print('search path was:\n-', '\n- '.join(sys.path))
                        print('interpreter:', sys.executable)

                    """
                ).strip()
            )
    print("Done installing hooks.")


def uninstall() -> None:
    print("uninstalling git hooks")
    hooks_dir = get_hooks_dir()
    for command in actions.keys():
        print(f"  uninstalling '{command}'")
        os.unlink(hooks_dir / command)


def main():
    if "uninstall" in sys.argv:
        uninstall()
    else:
        install()
