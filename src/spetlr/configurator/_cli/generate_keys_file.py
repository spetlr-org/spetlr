import argparse
import os.path

from spetlr.exceptions.cli_exceptions import SpetlrCliCheckFailed


def setup_parser(parser: argparse.ArgumentParser):
    parser.set_defaults(func=generate_keys_file)
    parser.add_argument("-o", "--output-file", type=str, default="")
    parser.add_argument(
        "-c",
        "--check-only",
        dest="check",
        action="store_true",
        help="Only check, don't update the output file",
    )
    parser.set_defaults(check=False)


def generate_keys_file(options):
    """Generate a keys file from configured keys.
    The arguments are as follows:
        - options is a SimpleNamespace, expected to contain these attributes
            - output_file - str - the name of the output file, if none given,
              the generate file contents are printed to the console
            - check True/False, don't generate the file, verify an existing one.
    """
    from spetlr import Configurator  # prevent circular import

    c = Configurator()
    new_conts = "\n".join(
        ["# AUTO GENERATED FILE", "# contains all spetlr.Configurator keys", ""]
        + [f"{key} = {repr(key)}" for key in sorted(c.all_keys())]
    )

    # if black is installed, use it to format the contents
    try:
        import black

        new_conts = black.format_file_contents(
            new_conts, fast=False, mode=black.FileMode()
        )
    except ModuleNotFoundError:
        pass

    output_file = options.output_file or ""
    output_file = output_file.replace("\\", "/")

    if not output_file:
        # without output file, output to console
        print(new_conts, end="")
        return

    if not options.check:
        with open(output_file, "w") as f:
            f.write(new_conts)
        return

    if not os.path.exists(output_file):
        raise SpetlrCliCheckFailed(f"Output file {output_file} does not exist.")

    old_conts = open(output_file).read()
    if new_conts != old_conts:
        raise SpetlrCliCheckFailed(
            f"Output file {output_file} does not have correct contents."
        )

    # all checks passed. Return without error.
    return
