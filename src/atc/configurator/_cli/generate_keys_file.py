import argparse
import os.path
from io import StringIO
from textwrap import dedent


def setup_parser(parser: argparse.ArgumentParser):
    parser.set_defaults(func=generate_keys_file)
    parser.add_argument("-o", "--output_file", type=str, default="")


def generate_keys_file(options):
    from atc import Configurator

    c = Configurator()

    writer = StringIO()
    writer.write(
        dedent(
            """\
                # AUTO GENERATED FILE.
                # contains all atc.Configurator keys

            """
        )
    )

    for key in c.all_keys():
        writer.write(f"{key} = {repr(key)}\n")

    writer.seek(0)
    new_conts = writer.read()

    try:
        import black

        new_conts = black.format_file_contents(
            new_conts, fast=False, mode=black.FileMode()
        )
    except ModuleNotFoundError:
        pass

    if not options.output_file:
        print(new_conts, end="")
        return 0
    else:
        if os.path.exists(options.output_file):
            old_conts = open(options.output_file).read()
        else:
            old_conts = ""
        with open(options.output_file, "w") as f:
            f.write(new_conts)
        if not old_conts or new_conts == old_conts:
            return 0
        else:
            return 1
