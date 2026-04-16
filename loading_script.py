'''
loading_script.py

This script connects to a Snowflake database, loads movie data from local files, and writes the data to Snowflake tables. It uses the Snowflake Connector for Python and pandas for data manipulation.
'''

# Imports
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import os

load_dotenv()

# Snowflake connection parameters
ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
USER = os.getenv('SNOWFLAKE_USER')
PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
WAREHOUSE = 'COMPUTE_WH'
DATABASE = 'P_MOVIE_DB'
SCHEMA = 'BRONZE'

# Connect to Snowflake
print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    account=ACCOUNT,
    user=USER,
    password=PASSWORD,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
)
print("Connected to Snowflake")

# Drop tables
print("Dropping existing tables if they exist...")
conn.cursor().execute("DROP TABLE IF EXISTS IMDB_BASIC_IMPORT")
conn.cursor().execute("DROP TABLE IF EXISTS IMDB_RATINGS_IMPORT")
conn.cursor().execute("DROP TABLE IF EXISTS LETTERBOXD_BASIC_IMPORT")
conn.cursor().execute("DROP TABLE IF EXISTS LETTERBOXD_POSTERS_IMPORT")

# Load Data
print("Loading data from local files...")
imdb_basic_df = pd.read_table('raw_data/title.basics.tsv', sep='\t')
imdb_ratings_df = pd.read_table('raw_data/title.ratings.tsv', sep='\t')
lb_basic_df = pd.read_csv('raw_data/movies.csv')
lb_posters_df = pd.read_csv('raw_data/posters.csv')

# Rename Cols for Snowflake
imdb_basic_df.columns = imdb_basic_df.columns.str.upper().str.replace(' ', '_')
imdb_ratings_df.columns = imdb_ratings_df.columns.str.upper().str.replace(' ', '_')
lb_basic_df.columns = lb_basic_df.columns.str.upper().str.replace(' ', '_')
lb_posters_df.columns = lb_posters_df.columns.str.upper().str.replace(' ', '_')
print("Data loaded and columns renamed for Snowflake.")

# Write to Snowflake
print("Writing data to Snowflake...")
print("IMDB Basic...")
write_pandas(conn, imdb_basic_df, 'IMDB_BASIC_IMPORT', auto_create_table=True)
print("IMDB Ratings...")
write_pandas(conn, imdb_ratings_df, 'IMDB_RATINGS_IMPORT', auto_create_table=True)
print("Letterboxd Basic...")
write_pandas(conn, lb_basic_df, 'LETTERBOXD_BASIC_IMPORT', auto_create_table=True)
print("Letterboxd Posters...")
write_pandas(conn, lb_posters_df, 'LETTERBOXD_POSTERS_IMPORT', auto_create_table=True)

print("Done.")    