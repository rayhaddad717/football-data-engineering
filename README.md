# football-data-engineering

# Create Virtual Environment

`python -m venv myenv`

# Run Project

`./myenv/Scripts/python main.py`
OR
activate the venv `source myenv/Scripts/activate`

# Install Airflow

`pip install apache-airflow`
Airflow username/password: admin/admin

# Install Beautiful Soup

`pip install bs4`

# Install pandas

`pip install pandas`

# Install Geopy

`pip install geopy`

# Make the entrypoint executable

`chmod +x script/entrypoint.sh`

# Create requirements.txt

`pip freeze > requirements.txt`

## After adding a new package in pip, recreate the requirements.txt and restart the docker containers

# Build docker containers

`docker compose up -d`

# Data Source

## The data is from wikipedia

https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity
