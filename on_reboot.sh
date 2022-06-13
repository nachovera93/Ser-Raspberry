#!/bin/bash

echo "loading virtualenvwrapper.sh..."  
source `which virtualenvwrapper.sh`
echo "accessing virtualenv..."
workon py3cv4
cd /home/pi/Desktop/Ser-Raspberry
echo "running Python script..."
python pruebabash.py
python medidorProduccion.py
echo "script exiting..." 