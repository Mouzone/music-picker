import csv
import sqlite3

def csv_to_sqlite(csv_filepath, sqlite_filepath, table_name, column_types):
    """
    Imports data from a CSV file into an SQLite database table with specified column types,
    including an auto-incrementing ID, and converts "Yes"/"No" to boolean for "Explicit".

    Args:
        csv_filepath (str): The path to the CSV file.
        sqlite_filepath (str): The path to the SQLite database file.
        table_name (str): The name of the table to create or append to.
        column_types (dict): A dictionary where keys are column names (matching the CSV header)
                             and values are the desired SQLite data types (e.g., 'INTEGER', 'REAL', 'TEXT').
    """
    try:
        with open(csv_filepath, 'r', newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile)
            header = next(csv_reader)  # Read the header row

            conn = sqlite3.connect(sqlite_filepath)
            cursor = conn.cursor()

            # Construct the CREATE TABLE statement with specified column types and auto-incrementing ID
            columns_definition = ['"id" INTEGER PRIMARY KEY AUTOINCREMENT']  # Define the ID column
            for col_name in header:
                data_type = column_types.get(col_name.strip(), 'TEXT')  # Default to TEXT if not specified
                columns_definition.append(f'"{col_name.strip()}" {data_type}')
            create_table_query = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(columns_definition)})'
            cursor.execute(create_table_query)

            # Construct the INSERT INTO statement (only for the columns from the CSV)
            placeholders = ', '.join('?' * len(header))
            insert_query = f'INSERT INTO "{table_name}" ({", ".join(f'"{col.strip()}"' for col in header)}) VALUES ({placeholders})'

            # Insert data rows, converting "Explicit" to boolean
            for row in csv_reader:
                # Convert 'Yes'/'No' to True/False for the 'Explicit' column
                modified_row = []
                for i, value in enumerate(row):
                    if header[i].strip() == 'Explicit':
                        if value.lower() == 'yes':
                            modified_row.append(True)
                        elif value.lower() == 'no':
                            modified_row.append(False)
                        else:
                            modified_row.append(value)  # Keep original if not 'Yes' or 'No'
                    else:
                        modified_row.append(value)
                cursor.execute(insert_query, modified_row)

            conn.commit()
            print(f"Successfully imported '{csv_filepath}' to table '{table_name}' in '{sqlite_filepath}' with an auto-incrementing ID, converting 'Explicit' to boolean.")

    except FileNotFoundError:
        print(f"Error: CSV file not found at '{csv_filepath}'.")
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    csv_file = 'light_spotify_dataset.csv'
    db_file = 'database.db'
    table = 'song_data'

    # Define the desired column types
    column_types_mapping = {
        'variance': 'REAL',
        'Release Date': 'INTEGER',
        'Tempo': 'INTEGER',
        'Loudness': "REAL",
        'Popularity': "INTEGER",
        'Energy': "INTEGER",
        'Danceability': "INTEGER",
        'Positiveness': "INTEGER",
        'Speechiness': "INTEGER",
        'Liveness': "INTEGER",
        'Acousticness': "INTEGER",
        'Instrumentalness': "INTEGER",
        'Explicit': 'INTEGER'  # Store boolean as INTEGER (1 for True, 0 for False)
    }

    csv_to_sqlite(csv_file, db_file, table, column_types_mapping)
