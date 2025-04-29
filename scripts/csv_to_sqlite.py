import csv
import sqlite3

def csv_to_sqlite(csv_filepath, sqlite_filepath, table_name):
    """
    Imports data from a CSV file into an SQLite database table.

    Args:
        csv_filepath (str): The path to the CSV file.
        sqlite_filepath (str): The path to the SQLite database file.
        table_name (str): The name of the table to create or append to.
    """
    try:
        with open(csv_filepath, 'r', newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile)
            header = next(csv_reader)  # Read the header row

            conn = sqlite3.connect(sqlite_filepath)
            cursor = conn.cursor()

            # Construct the CREATE TABLE statement based on the header
            columns = ', '.join(f'"{col}" TEXT' for col in header)
            create_table_query = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns})'
            cursor.execute(create_table_query)

            # Construct the INSERT INTO statement
            placeholders = ', '.join('?' * len(header))
            insert_query = f'INSERT INTO "{table_name}" VALUES ({placeholders})'

            # Insert data rows
            for row in csv_reader:
                cursor.execute(insert_query, row)

            conn.commit()
            print(f"Successfully imported '{csv_filepath}' to table '{table_name}' in '{sqlite_filepath}'.")

    except FileNotFoundError:
        print(f"Error: CSV file not found at '{csv_filepath}'.")
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    csv_file = '../light_spotify_dataset.csv'
    db_file = '../database.db'
    table = 'song'

    csv_to_sqlite(csv_file, db_file, table)
