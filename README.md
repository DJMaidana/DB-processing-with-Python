# DB-processing-with-Python
Downloads and sorts cultural information from Argentina's public database
## Requirements

- Python 3.8
- Python-decouple
- Pandas
- Psycopg2
- Sqlalchemy

## Installation

1. Create a virtual environment in a folder of your choosing with the following command: 

`python3 -m venv example_venv`

2. Change folder to the newly created virtual environment with `cd your-venv-name`, clone the repository with the following command:

`git clone https://github.com/DJMaidana/DB-processing-with-Python`

3. Still inside the root folder, activate the virtual environment with:

`source bin/activate`

4. Change folder with `cd DB-processing-with-Python` and install the required dependencies with:

`pip install -r requirements.txt`
 
5. Make sure your PostgreSQL Database configuration is correct, use the settings.ini file to set up the connection. The following are the default values:

```
POSTGRES_DB_NAME = alkemychallenge
POSTGRES_USER_NAME = postgres
POSTGRES_PASSWORD = postgres
POSTGRES_HOST = localhost
POSTGRES_PORT = 5432
```

6. Run the program:

`python3 main.py`
