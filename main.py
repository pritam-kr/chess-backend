from asyncio import constants
import psycopg2
from psycopg2 import sql
import requests
from typing import Union, List, Annotated
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import json
import asyncio
import httpx
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os

load_dotenv()

token = os.getenv("API_TOKEN")


from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    MetaData,
    Table,
    JSON,
    Boolean,
)
from databases import Database

app = FastAPI()
origins = [
    "http://localhost",
    "http://localhost:3000",  # Add the addresses of your frontend applications
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
tableNameForRating = "ratingHistory"
# Create a cursor
cursor = connection.cursor()

# Replace the database URL with your PostgreSQL connection string
DATABASE_URL = "postgresql://postgres:18218910p@localhost:5432/postgres"

# Connect to the database
database = Database(DATABASE_URL)
metadata = MetaData()


class Players(BaseModel):
    id: str
    username: str
    rating: int
    progress: int
    title: str
    online: bool


# get Players from DataBase
def getPlayersFromDataBase():
    query = f"SELECT id, username, rating, progress, title, online FROM {tableName};"
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    # Process the rows and create a list of dictionaries
    key_value_list = [{columns[i]: row[i] for i in range(len(columns))} for row in rows]
    return key_value_list


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
def checkPlayerTableIsAlreadyCreated():
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


def checkRatingHistoryTableAlreadyCreated():
    check_table_query = sql.SQL(
        """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = %s
    )
    """
    )

    cursor.execute(check_table_query, (tableNameForRating,))
    table_exists = cursor.fetchone()[0]
    return table_exists


def createTableIntoDatabase(data):
    if not checkPlayerTableIsAlreadyCreated():
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


def extractUserRatingHistory(username="Super_BrainPower"):
    api_url = f"https://lichess.org/api/user/Super_BrainPower/rating-history"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",  # Adjust content type if needed
    }

    try:
        response = requests.get(api_url, headers=headers)
        print(response.json())
        # print(username)
        return

    except Exception as e:
        print(f"An unexpected error occurred: {e}")


extractUserRatingHistory()

for item in getPlayersFromDataBase():
    # extractUserRatingHistory(item["username"])
    """"""


# Get 200 Players from API
def getClassicalUsers():
    try:
        api_url = "https://lichess.org/api/player/top/500/classical"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",  # Adjust content type if needed
        }
        response = requests.get(api_url, headers=headers)
        data = response.json()["users"]

        createTableIntoDatabase(data)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# getClassicalUsers()


# Fast API Things
@app.get("/players")
def getPlayers():
    try:
        lists = getPlayersFromDataBase()
        if lists:
            return {"players": lists}
        else:
            return {"Error": "Something went wrong, Please try again later"}

    except Exception as e:
        return {"Error": f"Something went wrong, {str(e)}"}


@app.get("/top-players")
def to50Players():
    try:
        lists = sorted(
            getPlayersFromDataBase(), key=lambda x: x["rating"], reverse=True
        )[:50]

        if lists:
            return {"top-players": lists}
        else:
            return {"Error": "Something went wrong, Please try again later"}

    except Exception as e:
        return {"Error": f"Something went wrong, {str(e)}"}
