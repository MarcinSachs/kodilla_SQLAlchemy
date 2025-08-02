from sqlalchemy import select, text
from schema import engine, create_tables, stations, measurements
from data_loader import load_stations_from_csv, load_measurements_from_csv


def db_setup():
    """Sets up the database by creating tables and loading initial data."""
    create_tables()
    print("Database setup complete.")
    # 2. Use a connection context to load data
    with engine.connect() as connection:
        # Check if the stations table is empty to avoid duplicates
        if not connection.execute(select(stations).limit(1)).first():
            print("Loading stations from CSV...")
            stations_data = list(load_stations_from_csv("clean_stations.csv"))
            if stations_data:
                connection.execute(stations.insert(), stations_data)
                connection.commit()
            print("Stations loaded.")
        else:
            print("Stations table is not empty. Skipping station loading.")

        # Check if the measurements table is empty
        if not connection.execute(select(measurements).limit(1)).first():
            print("Loading measurements from CSV...")
            # Pass the connection so the function can fetch station IDs
            measurements_data = list(load_measurements_from_csv(
                connection, "clean_measure.csv"))
            if measurements_data:
                connection.execute(measurements.insert(), measurements_data)
                connection.commit()
            print("Measurements loaded.")
        else:
            print("Measurements table is not empty. Skipping measurement loading.")

    print("Data loading process finished.")


if __name__ == "__main__":
    # Create tables and load data into the database
    # db_setup()

    with engine.connect() as conn:
        # For raw SQL, use the text() construct as required by SQLAlchemy 2.0+
        query = text("SELECT * FROM stations LIMIT 5")
        result = conn.execute(query).fetchall()
        for row in result:
            print(row)
