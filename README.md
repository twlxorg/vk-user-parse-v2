## Install dependencies
It is highly recommended to use Poetry as package manager for that code, but you, probably, can use default pip.

If you're installing dependencies with pip it is highly recommended to use Virtual Environment for you Python.
```bash
python3 -m venv .venv
source .venv/bin/activate  # on Linux
./.venv/Scripts/Activate.ps1  # on Windows (PowerShell)
```

Then install dependencies
```bash
$ poetry install --no-root  # with Poetry
$ pip3 install -r requirements.txt  # with pip
```

## Run
Before execute verify, that you created **.env** file with your VK token.

```bash
python main.py
```
As a result **report.txt** will be created with collected information.

### Run with parameters
If you want, you're able to run program with two arguments:
```
python main.py -h
usage: main.py [-h] [--user_id USER_ID] [--output OUTPUT]

options:
  -h, --help         show this help message and exit
  --user_id USER_ID  Fetch data for specified user_id
  --output OUTPUT    Output file (.txt) path
```