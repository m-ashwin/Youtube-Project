
# YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit

Youtube Data Harvesting is used to fetch the data from the public youtube channels using youtube V3 api. We need to fetch the channel ID and add it to the GUI to fetch the necessary data of the respective channel. Once after harvesting, the harvested data is stored into MongoDB as a datalake, the selected data can be further migrated to MySQL and we can do few analysis on the migrated data.


## Acknowledgements

 - [GUVI](https://www.guvi.in/)
 - [Python](https://www.python.org/dev/peps/pep-0008/)
 - [Streamlit docs](https://docs.streamlit.io/)


## Tech Stack

**Language:** Python
**Libraries:** streamlit,google-api-python-client,pymongo,certifi,mysql-connector-python,pandas,Pillow
**SQL Database:**: MySQL
**NoSQL Database:**: MongoDB
**GUI Framework:** Streamlit


## Harvesting the channel data
To harvest channel data, youtube V3 api is used. Various endpoints are used to get respective data like channel, playlist, video and comments. All the endpoints require an API-key which can be created from google developer account. Each endpoint needs specific input to retrive the data.

## Uploading data in MongoDB
Once the data is scrapped, The data will be structured and constructed as a single embedded document. The document is then pushed into MongoDB, here we are using mongo as a data lake.

## Migrating data in MySQL
At the very next process, retrived the data from mongo and constructed it in the form of quries. Using these quries the data will be inserted in to SQL tables.

## Creating the UI
GUI is designed using Streamlit app for youtube data harvesting. The GUOI includes 4 menus named Home, Search, Question and About

**Menu 1 -- Home**  
Home page of the UI contains title of the app and an intro

**Menu 2 -- Search**  
Search menu is used to search the youtube channel with channel ID and migrate the scrapped data into SQL

**Menu 3 -- Questions**  
Questions menu is used to analyse the scrpped data with the few set of questions

**Menu 4 -- About**  
Under About menu, we can find few descriptions of the major components used
