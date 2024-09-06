#!/bin/bash

sudo apt-get update
sudo apt-get -f install -y
sudo ACCEPT_EULA=Y apt-get install -q -y /dbfs/databricks/drivers/msodbcsql18_amd64.deb

ldd /opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.0.so.1.1

echo "[ODBC Driver 18 for SQL Server]" | sudo tee -a /etc/odbcinst.ini
echo "Description=Microsoft ODBC Driver 18 for SQL Server" | sudo tee -a /etc/odbcinst.ini
echo "Driver=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.0.so.1.1" | sudo tee -a /etc/odbcinst.ini

echo 'export PATH="$PATH:/opt/microsoft/msodbcsql18/bin"' | sudo tee -a ~/.bashrc
source ~/.bashrc