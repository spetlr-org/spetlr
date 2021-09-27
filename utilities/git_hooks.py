"""
This script installs and runs git commit hooks.
"""
import os
import os.path
import sys
from textwrap import dedent

import compare_version


def pre_commit():
    compare_version.main()


def install():
    print("Now installing hooks.")
    utils_dir = os.path.dirname(__file__)
    base_path = os.path.dirname(utils_dir)
    hooks_dir = os.path.join(base_path, ".git", "hooks")
    if not os.path.isdir(hooks_dir):
        raise AssertionError("Git hooks directory not found. Please run this script form the repo base.")

    for command in actions.keys():
        with open(os.path.join(hooks_dir, command), "w") as f:
            print(f"Installing hook for '{command}'")
            f.write(
                dedent(f"""
                    #!{sys.executable}
                    import sys
                    sys.path.append({repr(utils_dir)})
                    import git_hooks
                    git_hooks.actions[{repr(command)}]()
                """).strip()
            )
    print("Done installing hooks.")


actions = {
    'pre-commit': pre_commit
}


def main():
    install()

if __name__ == "__main__":
    main()
