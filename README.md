# Description

This is a trusted web scraper for extracting products from [Sainsburys](https://sainsburys.co.uk/) supermarket.

# Usage

This scraper only supports Ubuntu.

## Prerequisites

- [Python](https://phoenixnap.com/kb/how-to-install-python-3-ubuntu) installed

- [Git](https://www.digitalocean.com/community/tutorials/how-to-install-git-on-ubuntu-20-04) installed

- Selenium Grid Server installed on virtual private server with Windows

  - [Java 17](https://www.oracle.com/java/technologies/javase/jdk17-archive-downloads.html) or higher installed

  - [Microsoft Visual C++ Redistributable x86](https://aka.ms/vs/17/release/vc_redist.x86.exe) and [Microsoft Visual C++ Redistributable x64](https://aka.ms/vs/17/release/vc_redist.x64.exe) installed

  - Download the Selenium Server jar file from the [latest release](https://github.com/SeleniumHQ/selenium/releases/latest)

  - Start the Selenium Grid

    `java -jar selenium-server-<version>.jar standalone --selenium-manager true --port 9515`

## Get the code

`git clone https://github.com/mystery000/Tesco-Scraper.git` <br />

## Installation

- `sudo apt-get install python3.9-venv`

- `python3 -m venv venv`

- `source venv/bin/activate`

- `pip install requests html5lib beautifulsoup4 selenium python-dotenv pandas`

## Configuration

This scraper runs automatically at the time specified in `wathcer.txt`.<br />
Time format: `24H`

## How to run

- If you run this script in background, please use this command.

  `nohup python3 main.py &`

- To stop this script

  `pkill -f main.py`

- To look at the logs

  `cat nohup.out`
