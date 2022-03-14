#!/bin/bash

echo "loading virtualenvwrapper.sh..."  
source `which virtualenvwrapper.sh`
echo "accessing virtualenv..."
workon py3cv4
cd /home/pi/Desktop/MedidorMonofasico/Ser-Raspberry
echo "running Python script..."
python pruebabash.py
python medidorMonofasico.py
echo "script exiting..." 