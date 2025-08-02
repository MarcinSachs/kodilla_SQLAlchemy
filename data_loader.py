import csv
from datetime import datetime
from sqlalchemy import select
from schema import stations, measurements


def load_stations_from_csv(file_path):
    """Generator that loads stations from a CSV file as dictionaries."""
    with open(file_path, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Convert data types to the correct ones
            row["latitude"] = float(row["latitude"])
            row["longitude"] = float(row["longitude"])
            row["elevation"] = float(row["elevation"])
            yield row


def load_measurements_from_csv(connection, file_path):
    """
    Generator that loads measurements from a CSV file as dictionaries.
    Uses an existing connection to fetch station IDs.
    """
    # 1. Fetch all stations and map their names to IDs in a single query
    stmt = select(stations.c.id, stations.c.station)
    result = connection.execute(stmt)
    station_id_map = {name: id for id, name in result}

    with open(file_path, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            station_name = row["station"]
            station_id = station_id_map.get(station_name)

            if station_id:
                # Corrected column names and handling of empty values
                precip_val = row.get("precip")
                tobs_val = row.get("tobs")

                yield {
                    "station_id": station_id,
                    "date": datetime.strptime(row["date"], "%Y-%m-%d").date(),
                    "precipitation": float(precip_val) if precip_val else None,
                    "temperature_observed": float(tobs_val) if tobs_val else None,
                }
            else:
                print(
                    f"Warning: Station named '{station_name}' not found. Skipping record."
                )
