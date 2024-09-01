import sqlite3

# SQLite database connection
conn = sqlite3.connect('weather_data_updated.db')
cursor = conn.cursor()


# Function to clear the weather_data table with user confirmation
def clear_table():
    # Ask the user for confirmation
    confirmation = input("WARNING: This will delete ALL records from the weather_data table! 🌪️💾\n"
                         "Are you absolutely, positively, 100% sure you want to do this? (type 'Y' for Yes, 'N' for No): ").strip().upper()

    if confirmation == 'Y':
        cursor.execute('DELETE FROM weather_data')
        conn.commit()
        print("All data cleared from the weather_data table. 🧹✨ Your database is now squeaky clean!")
    else:
        print("Phew! That was close! 😅 Your data is safe... for now.")


# Call the function to clear the table if confirmed
clear_table()

# Close the connection
conn.close()
