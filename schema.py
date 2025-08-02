from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    Date,
    ForeignKey,
)

engine = create_engine("sqlite:///database.db")
meta = MetaData()

stations = Table(
    "stations", meta,
    Column("id", Integer, primary_key=True),
    Column("station", String, unique=True, nullable=False),
    Column("latitude", Float, nullable=False),
    Column("longitude", Float, nullable=False),
    Column("elevation", Float, nullable=False),
    Column("name", String, nullable=False),
    Column("country", String, nullable=False),
    Column("state", String, nullable=True),
)

measurements = Table(
    "measurements", meta,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("station_id", Integer, ForeignKey("stations.id"), nullable=False),
    Column("date", Date, nullable=False),
    Column("precipitation", Float, nullable=True),
    Column("temperature_observed", Float, nullable=True),
)


def create_tables():
    """Creates tables in the database if they don't exist."""
    meta.create_all(engine)
