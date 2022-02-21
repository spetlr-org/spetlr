

Push-Location
$srcDir = "$PSScriptRoot/../.."
cd $srcDir

python -m pip install --upgrade pip
pip install setuptools wheel twine packaging

# back up the version file
Copy-Item "$srcDir/src/atc/__init__.py" -Destination "$srcDir/init.py"
python "$PSScriptRoot/reset_version.py" "$srcDir/src/atc/__init__.py"

if(Test-Path -Path dist){
    Remove-Item -Force -Recurse dist
}

python setup.py bdist_wheel
Remove-Item -Force -Recurse build

# restore the version file
Move-Item "$srcDir/init.py" -Destination "$srcDir/src/atc/__init__.py" -Force

Pop-Location

