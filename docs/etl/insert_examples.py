from pathlib import Path

examples_folder = Path(__file__).parent.parent.parent / "examples" / "etl"
examples = {
    file.stem: open(file).read()
    for file in examples_folder.iterdir()
    if file.suffix == ".py"
}

print(list(examples.keys()))
template = open(Path(__file__).parent / "README.template.md").read()
open(Path(__file__).parent / "README.md", "w").write(template.format(**examples))
