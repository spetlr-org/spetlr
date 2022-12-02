import argparse

from atc.exceptions.cli_exceptions import AtcCliException

from . import generate_keys_file


def cli(self):
    """
    This function exposes functionality to be used in code analysis and deployment.
    Initialize the configurator in your project like

    ```
    def init_my_configurator():
        c= Configurator()
        c.add_resource_path(my_yaml_module)
        c.register('ENV', config.my_env_name)
        return c

    def my_configurator_cli():
        init_my_configurator().cli()
    ```
    Then expose the `my_configurator_cli` as a command line tool.
    The cli has the following usage

    <my_cli> [action [action_options]]

    Available actions:

    - generate-keys-file [-o output_file]
        Generates a python file that contains every available key string as a python
        object of the same name. Including this file in your python project allows
        you to get auto-completion of key names.
        The output is dumped to stdout unless -o output_file is specified.
        The option -o has the side-effect of return an exit code 1 if the file was
        updated. This allows you to check for a correctly updated keys file in your
        CICD pipleine.
    """
    parser = argparse.ArgumentParser()
    subp = parser.add_subparsers(help="actions")

    generate_keys_file.setup_parser(subp.add_parser("generate-keys-file"))
    # add further parsers here

    options = parser.parse_args()

    try:
        func = options.func
    except AttributeError:
        parser.print_help()
        raise AtcCliException("No actions given")

    func(self, options)
