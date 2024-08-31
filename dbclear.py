import sqlite3

# Correct SQLite database connection
conn = sqlite3.connect('weather_data_updated.db')
cursor = conn.cursor()

# Function to clear the weather_data table
def clear_table():
    cursor.execute('DELETE FROM weather_data')
    conn.commit()
    print("All data cleared from the weather_data table.")

# Call the function to clear the table
clear_table()

# Close the connection
conn.close()

