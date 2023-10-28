def bformat(input: str) -> str:
    """Format the input python code with black if
    black is installed. If it is not installed, just
    return the input."""
    try:
        import black

        return black.format_file_contents(input, fast=False, mode=black.FileMode())
    except ModuleNotFoundError:
        return input
