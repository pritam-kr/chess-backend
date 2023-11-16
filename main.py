from asyncio import constants
import psycopg2
from psycopg2 import sql
import requests

# from typing import Union, List, Annotated
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.responses import JSONResponse

# from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os

# Call the asynchronous function using an event loop
import asyncio
import httpx

load_dotenv()

token = os.getenv("API_TOKEN")
# from sqlalchemy import (
#     MetaData,
# )
# from databases import Database

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
    select_query = f"SELECT * FROM {tableNameForRating};"
    cursor.execute(select_query)

    # Fetch all the rows from the result set
    rows = cursor.fetchall()
    print(rows)

    # Print the retrieved data
    # for row in rows:
    # print(row)


# getTableData()


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


async def extractUserRatingHistory(username="Super_BrainPower", token=token):
    api_url = f"https://lichess.org/api/user/{username}/rating-history"
    headers = {
        "Authorization": f"Bearer {token}",
        # "Content-Type": "application/json",  # Adjust content type if needed
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(api_url, headers=headers)
            response.raise_for_status()
            return response.json()
            # Process the response as needed
        except httpx.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")


def background_task(username):
    rating_history = asyncio.run(extractUserRatingHistory(username))
    insert_script = sql.SQL(
        """
        INSERT INTO ratingHistory (id, Bullet, Rapid, Classical, Correspondence, Chess960, "King of the Hill",  "Three-check", Antichess, Atomic, Horde, "Racing Kings", Crazyhouse, Puzzles, UltraBullet)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    ).format(sql.Identifier(tableNameForRating))

    for item in rating_history:

        def getTableValue(rowname):
            if item["name"] == "Bullet":
                return item["points"]
            elif item["name"] == "Rapid":
                return item["points"]
            elif item["name"] == "Classical":
                return item["points"]
            elif item["name"] == "Correspondence":
                return item["points"]
            elif item["name"] == "Chess960":
                return item["points"]
            elif item["name"] == "King of the Hill":
                return item["points"]
            elif item["name"] == "Three-check":
                return item["points"]
            elif item["name"] == "Antichess":
                return item["points"]
            elif item["name"] == "Atomic":
                return item["points"]
            elif item["name"] == "Horde":
                return item["points"]
            elif item["name"] == "Racing Kings":
                return item["points"]
            elif item["name"] == "Crazyhouse":
                return item["points"]
            elif item["name"] == "Puzzles":
                return item["points"]
            elif item["name"] == "UltraBullet":
                return item["points"]

        # insert_value = (
        #     username,
        #     getTableValue("Bullet"),
        #     getTableValue("Rapid"),
        #     getTableValue("Classical"),
        #     getTableValue("Correspondence"),
        #     getTableValue("Chess960"),
        #     getTableValue("King of the Hill"),
        #     getTableValue("Three-check"),
        #     getTableValue("Antichess"),
        #     getTableValue("Atomic"),
        #     getTableValue("Horde"),
        #     getTableValue("Racing Kings"),
        #     getTableValue("Crazyhouse"),
        #     getTableValue("Puzzles"),
        #     getTableValue("UltraBullet"),
        # )


        insert_value = (
            username,
            [1],
            [2],
            [3],
            [4],
            [5],
            [6],
            [7],
            [8],
            [9],
            [10],
            [11],
            [12],
            [13],
            [14]
        )

        cursor.execute(insert_script, insert_value)
    connection.commit()


@app.get("/fetch-data/{username}")
async def fetch_data(username: str, background_tasks: BackgroundTasks):
    if not checkRatingHistoryTableAlreadyCreated():
        create_script = """
            CREATE TABLE IF NOT EXISTS ratingHistory (
            id VARCHAR PRIMARY KEY,
            Bullet integer[],
            Rapid integer[], 
            Classical integer[],
            Correspondence integer[],
            Chess960 integer[],
            "King of the Hill" integer[],
            "Three-check" integer[],
            Antichess integer[],
            Atomic integer[],
            Horde integer[],
            "Racing Kings" integer[],
            Crazyhouse integer[],
            Puzzles integer[],
            UltraBullet integer[]
            )
        """
        #cursor.execute(create_script)
        #connection.commit()
    else:
        print("Table Created for rating history")

    # for item in getPlayersFromDataBase():
    #     background_tasks.add_task(background_task, item["username"])
    background_tasks.add_task(background_task, username)

    #
    # top_players = await extractUserRatingHistory()
    return JSONResponse(content={"message": "Data fetching initiated"})


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
        print(data)

        # createTableIntoDatabase(data)
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
