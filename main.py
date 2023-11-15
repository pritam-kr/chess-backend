from asyncio import constants
import psycopg2
from psycopg2 import sql
import requests
from typing import Union, List, Annotated
from fastapi import FastAPI, HTTPException, Depends
import json
import asyncio
import httpx

from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table
from databases import Database

app = FastAPI()

# Making Database connection
connection = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="18218910p",
    port=5432,
)
try:
    print("Data base connected")
except psycopg2.Error as e:
    print("Error connecting to the database:", e)

# TABLE name in Database
tableName = "players"
# Create a cursor
cursor = connection.cursor()


# Close data base connection
def closeDataBaseConnection():
    # Close the cursor and connection
    cursor.close()
    connection.close()


# DELETE A Table from database
def deleteTable():
    # Drop the table if it exists
    drop_table_query = f"DROP TABLE IF EXISTS {tableName};"
    cursor.execute(drop_table_query)

    # Commit the changes to the database
    connection.commit()


# RETRIEVE Data from Table
def getTableData():
    # Execute a SELECT query to retrieve data from the table
    select_query = f"SELECT * FROM {tableName};"
    cursor.execute(select_query)

    # Fetch all the rows from the result set
    rows = cursor.fetchall()
    print(rows)

    # Print the retrieved data
    # for row in rows:
    # print(row)


# Checking table is already created or not
def checkTableIsAlreadyCreated():
    check_table_query = sql.SQL(
        """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = %s
    )
    """
    )

    cursor.execute(check_table_query, (tableName,))
    table_exists = cursor.fetchone()[0]
    return table_exists


def createTableIntoDatabase(data):
    if not checkTableIsAlreadyCreated():
        create_script = """
        CREATE TABLE IF NOT EXISTS players (
        id VARCHAR PRIMARY KEY,
        username VARCHAR, 
        rating INTEGER,
        progress INTEGER,
        title VARCHAR,
        online BOOLEAN
        )
    """
        cursor.execute(create_script)

        for item in data:
            insert_script = sql.SQL(
                """
            INSERT INTO players (id, username, rating, progress, title, online)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            ).format(sql.Identifier(tableName))

            insert_value = (
                item["id"],
                item["username"],
                item["perfs"]["classical"]["rating"],
                item["perfs"]["classical"]["progress"],
                item.get("title", None),
                item.get("online", None),
            )

            cursor.execute(insert_script, insert_value)

        connection.commit()
        # Close the cursor and connection
        # closeDataBaseConnection()

    else:
        print("Table is already Created")


def extractUserRatingHistory(username="apodex64"):
    access_token = "lip_FJbk2xc8lE26MjBX0vF6"
    api_url = f"https://lichess.org/api/user/{username}/rating-history"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",  # Adjust content type if needed
    }

    try:
        response = requests.get(api_url, headers=headers)
        print(response.json())

    except Exception as e:
        print(f"An unexpected error occurred: {e}")


extractUserRatingHistory()


# Fetch an API
def getClassicalUsers():
    try:
        access_token = "lip_FJbk2xc8lE26MjBX0vF6"
        api_url = "https://lichess.org/api/player/top/500/classical"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",  # Adjust content type if needed
        }
        response = requests.get(api_url, headers=headers)
        data = response.json()["users"]

        createTableIntoDatabase(data)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


getClassicalUsers()


# Fast API Things
@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}
