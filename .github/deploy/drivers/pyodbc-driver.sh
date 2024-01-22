#!/bin/bash

echo "Install package..."
sudo apt-get update && sudo apt-get install -y lsb-release

echo "Checking Ubuntu version..."
 
# Get the release number
release=$(lsb_release -r | awk '{print $2}')

# Check if the release number matches 20.04 or 22.04 and execute the corresponding command
if [ "$release" = "20.04" ]; then
    echo "Detected Ubuntu 20.04. Installing package for 20.04..."
    sudo ACCEPT_EULA=Y apt-get install -q -y /dbfs/databricks/drivers/msodbcsql18_amd64_ubuntu_20_04.deb
elif [ "$release" = "22.04" ]; then
    echo "Detected Ubuntu 22.04. Installing package for 22.04..."
    sudo ACCEPT_EULA=Y apt-get install -q -y /dbfs/databricks/drivers/msodbcsql18_amd64_ubuntu_22_04.deb
else
    echo "This script is intended for Ubuntu 20.04 or 22.04. Your version is $release."
fi