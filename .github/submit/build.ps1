
$srcDir = "$PSScriptRoot/../.."

Push-Location -Path $srcDir

python -m pip install --upgrade pip
pip install -r requirements_test.txt

# we want a really clean build (even locally)
if (Test-Path -Path dist) {
    Remove-Item -Force -Recurse dist
}
if (Test-Path -Path build) {
    Remove-Item -Force -Recurse build
} 
if (Test-Path -Path src\spetlr.egg-info) {
    Remove-Item -Force -Recurse src\spetlr.egg-info
}
  
pyclean -v .

python setup.py bdist_wheel

Pop-Location