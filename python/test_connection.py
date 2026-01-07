import mysql.connector
import pandas as pd

conn = mysql.connector.connect(
    host="localhost",
    user="analytics_user",
    password="Analytics@123",
    database="restaurant_db",
    port=3306
)

# sanity check
df = pd.read_sql("SELECT COUNT(*) AS orders FROM order_details;", conn)
print(df)

conn.close()
