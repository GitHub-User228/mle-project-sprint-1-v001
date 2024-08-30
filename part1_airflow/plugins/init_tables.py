from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Boolean,
    Integer,
    Float,
    UniqueConstraint,
)


metadata = MetaData()
tables = {}

tables["prepared_flats"] = Table(
    "prepared_flats",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("flat_id", Integer),
    Column("floor", Integer),
    Column("is_apartment", Boolean),
    Column("kitchen_area", Float),
    Column("living_area", Float),
    Column("rooms", Integer),
    Column("studio", Boolean),
    Column("total_area", Float),
    Column("target", Float),
    Column("building_id", Integer),
    Column("build_year", Integer),
    Column("building_type_int", Integer),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("ceiling_height", Float),
    Column("flats_count", Integer),
    Column("floors_total", Integer),
    Column("has_elevator", Boolean),
    UniqueConstraint("flat_id", name="unique_flat_id_constraint"),
)

tables["cleaned_flats"] = Table(
    "cleaned_flats",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("flat_id", Integer),
    Column("floor", Integer),
    Column("is_apartment", Boolean),
    Column("kitchen_area", Float),
    Column("living_area", Float),
    Column("rooms", Integer),
    Column("total_area", Float),
    Column("log1p_target", Float),
    Column("building_id", Integer),
    Column("build_year", Integer),
    Column("building_type_int", Integer),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("ceiling_height", Float),
    Column("flats_count", Integer),
    Column("floors_total", Integer),
    Column("has_elevator", Boolean),
    Column("is_duplicated", Boolean),
    UniqueConstraint("flat_id", name="unique_flat_id_constraint_2"),
)
