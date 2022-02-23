
$srcDir = "$PSScriptRoot/../.."

Push-Location -Path $srcDir

python -m pip install --upgrade pip
pip install setuptools wheel

# we want a really clean build (even locally)
if(Test-Path -Path dist){
    Remove-Item -Force -Recurse dist
}

python setup.py bdist_wheel


Pop-Location

