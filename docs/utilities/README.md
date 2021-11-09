# Utilities documentation

Utilities in atc-dataplatform:

* [Git Hooks](#git-hooks)

## Git Hooks

A set of standard git hooks are included to provide useful functionality

- *pre-commit* before every commit, all files ending in `.py` will be formatted with the black code formatter

To use the hooks, they can be installed in any repository by executing this command from a path inside the repository:

    atc-dataplatform-git-hooks

To uninstall the hooks, simply run this command

    atc-dataplatform-git-hooks uninstall

