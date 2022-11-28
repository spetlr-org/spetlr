import argparse
import os.path
from io import StringIO


def setup_parser(parser: argparse.ArgumentParser):
    parser.set_defaults(func=generate_keys_file)
    parser.add_argument("-o", "--output_file", type=str, default="")
    parser.add_argument(
        "-c",
        "--check-only",
        dest="check",
        action="store_true",
        help="Only check, don't update the output file",
    )
    parser.set_defaults(check=False)


def generate_keys_file(self, options):

    writer = StringIO()
    writer.write("# AUTO GENERATED FILE\n# contains all atc.Configurator keys\n\n")

    for key in self.all_keys():
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
        if options.check:
            if os.path.exists(options.output_file):
                old_conts = open(options.output_file).read()
                if new_conts == old_conts:
                    return 0
                elif options.check:
                    print(
                        f"Output file {options.output_file} "
                        "does not have correct contents."
                    )
                    return 1
            else:
                print(f"Output file {options.output_file} does not exist.")
                return 1
        with open(options.output_file, "w") as f:
            f.write(new_conts)
        return 0
