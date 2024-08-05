import json
from geopy import Nominatim
import pandas as pd

NO_IMAGE = "https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png"


def get_wikipedia_page(url):
    import requests

    print("Getting wikipedia page...", url)
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # check if the request is successful
        return response.text
    except requests.RequestException as e:
        print("Failed to get wikipedia page:", e)
        return None


def get_wikipedia_data(html):
    from bs4 import BeautifulSoup

    print("Extracting wikipedia data...")
    soup = BeautifulSoup(html, "html.parser")
    all_tables = soup.find_all("table")
    if len(all_tables) == 0:
        print("No tables found on the wikipedia page")
        return ""
    football_table = None
    for table in all_tables:
        if table.find("caption") and "football" in table.find("caption").text.lower():
            football_table = table
            break
    if not football_table:
        print("No football table found")
        return ""
    table_rows = football_table.find_all("tr")
    return table_rows


def clean_text(text):
    text = str(text).strip().replace("&nbsp", "")
    if text == "\n":
        return ""
    keys_to_remove = ["♦", "[", "(formerly)"]
    for key in keys_to_remove:
        if key in text:
            text = text.split(key)[0]
    return text.strip().replace("\n", "")


def extract_wikipedia_data(**kwargs):
    url = kwargs["url"]
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)
    data = []
    for i in range(1, len(rows)):
        # skip the header, start from index 1
        row = rows[i]
        columns = row.find_all("td")
        if len(columns) == 0:
            continue
        values = {
            "rank": i,
            "stadium": clean_text(columns[0].text),
            "capacity": clean_text(columns[1].text).replace(",", "").replace(".", ""),
            "region": clean_text(columns[2].text),
            "country": clean_text(columns[3].text),
            "city": clean_text(columns[4].text),
            "images": (
                "https://" + columns[5].find("img").get("src").split("//")[1]
                if columns[5].find("img")
                else "NO_IMAGE"
            ),
            "home_team": clean_text(columns[6].text),
        }
        data.append(values)
    json_rows = json.dumps(data)
    """
    Breakdown
    kwargs["ti"]:

    kwargs is a dictionary of keyword arguments passed to the function.
    ti stands for "task instance" and is a common key in Airflow's context dictionary. It represents the current task instance.
    .xcom_push(key="wikipedia_data", value=json_rows):

    xcom_push is a method of the task instance (ti) used to push data to an XCom.
    key="wikipedia_data" specifies the key under which the data will be stored. This key can be used later to retrieve the data.
    value=json_rows is the data being pushed to the XCom. In this case, json_rows is the value being stored.
    Purpose
    This line is used to store json_rows in an XCom with the key "wikipedia_data". This allows other tasks in the Airflow DAG (Directed Acyclic Graph) to retrieve this data using the same key """
    kwargs["ti"].xcom_push(key="rows", value=json_rows)
    print("DONE")

    return "OK"


def get_lat_long(country, city):
    geolocator = Nominatim(user_agent="rays__geoapiExercises__dataengineering_project")
    location = geolocator.geocode(f"{city}, {country}")
    if location:
        return location.latitude, location.longitude
    return None


def transform_wikipedia_data(**kwargs):
    data = kwargs["ti"].xcom_pull(key="rows", task_ids="extract_data_from_wikipedia")
    data = json.loads(data)
    stadium_df = pd.DataFrame(data)
    stadium_df["images"] = stadium_df["images"].apply(
        lambda x: x if x not in ["NO_IMAGE", "", None] else NO_IMAGE
    )
    stadium_df["capacity"] = stadium_df["capacity"].astype(int)
    """
    The axis=1 parameter in the apply method of a Pandas DataFrame specifies that the function should be applied to each row, rather than each column.
    """
    stadium_df["location"] = stadium_df.apply(
        # lambda x: get_lat_long(x["country"], x["stadium"]),
        # axis=1,
        lambda x: x["country"] + ", " + x["city"],
        axis=1,
    )
    # handle the duplicates
    duplicates = stadium_df[stadium_df.duplicated(["location"])]
    # if the stadiums have the same location, we need to update the location by passing the city instead of stadium name
    # duplicates["location"] = duplicates.apply(
    #     lambda x: get_lat_long(x["country"], x["city"]), axis=1
    # )
    """
    The line stadium_df.update(duplicates) updates the stadium_df DataFrame with values from the duplicates DataFrame.
    """
    stadium_df.update(duplicates)

    # push to xcom
    kwargs["ti"].xcom_push(key="rows", value=stadium_df.to_json())

    return "OK"


def write_wikipedia_data(**kwargs):
    from datetime import datetime

    data = kwargs["ti"].xcom_pull(key="rows", task_ids="transform_wikipedia_data")
    data = json.loads(data)
    data = pd.DataFrame(data)
    file_name = (
        "stadium_cleaned "
        + str(datetime.now().date())
        + "_"
        + str(datetime.now().time()).replace(":", "_")
        + ".csv"
    )
    # index false means that we don't want to write the index to the csv
    data.to_csv("data/" + file_name, index=False)
    return "OK"


def test_page(url):
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)
    print("Extracted", rows)
