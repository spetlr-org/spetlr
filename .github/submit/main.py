import argparse
import contextlib
import io
import os
import sys
import unittest


def run_all():
    parser = argparse.ArgumentParser(description="Run Test Cases.")
    parser.add_argument(
        "--basedir", type=str, required=True, help="parent location of test library"
    )
    parser.add_argument(
        "--folder", type=str, required=True, help="which test folder to run"
    )

    args = parser.parse_args()

    # process the logout path
    basedir = args.basedir
    if not str(basedir).startswith("dbfs:/"):
        print("WARNING: argument basedir must start with dbfs:/")
    else:
        basedir = f"/dbfs/{basedir[6:]}"

    os.chdir(basedir)

    sys.path = [os.getcwd()] + sys.path

    suite = unittest.TestLoader().discover(args.folder)
    with io.StringIO() as buf:
        # run the tests
        with contextlib.redirect_stdout(buf):
            res = unittest.TextTestRunner(stream=buf, failfast=True).run(suite)
        output = buf.getvalue()
        print(output)
        with open("results.log", "w") as f:
            f.write(output)

        return 0 if res.wasSuccessful() else 1


if __name__ == "__main__":
    if int(run_all()):
        sys.exit(1)