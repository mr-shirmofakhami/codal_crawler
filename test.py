import pandas as pd
import psycopg2
from sqlalchemy import create_engine

DATABASE_URL = "postgresql://postgres:123@172.20.2.91:5432/codal"
engine = create_engine(DATABASE_URL)


# Connect to database
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

# Query the tables
cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public';
""")

tables = cursor.fetchall()

table_name_1 = "stock_notices"
table_name_2 = "financial_statement_data"
table_name_3 = "users"

df_1 = pd.read_sql(f"SELECT * FROM {table_name_1};", engine)
df_2 = pd.read_sql(f"SELECT * FROM {table_name_2};", engine)
df_3 = pd.read_sql(f"SELECT * FROM {table_name_3};", engine)

# # Show nicely
# print("Tables in database:")
# for t in tables:
#     print("-", t[0])


print(df_1.head())

cursor.close()
conn.close()