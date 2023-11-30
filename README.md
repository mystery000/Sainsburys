# Sainsburys

This is a trusted web scraper for extracting products from Sainburys.

# How to setup

## Ubuntu

python3 -m venv venv
source venv/bin/activate

pip install requests
pip install html5lib
pip install beautifulsoup4
pip install selenium
pip install pandas

## Windows

python -m venv venv
venv\Scripts\activate

pip install requests
pip install html5lib
pip install beautifulsoup4
pip install selenium
pip install pandas

# How to run

## Ubuntu

source venv/bin/activate
nohup python3 main.py &

https://hinty.io/rivashchenko/run-python-script-in-background/#:~:text=If%20you%20want%20to%20run,prepare%20our%20script%20for%20this.&text=Then%20I%20recommend%20adding%20a,it%20is%20a%20python%20script.

## Windows

venv\Scripts\activate
python main.py
