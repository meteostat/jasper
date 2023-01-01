from pulire import Schema, validators


def get_template(schema: Schema):
    """
    Generate template dict from Pulire schema
    """
    return dict.fromkeys(schema.columns())

def get_sql(table: str, schema: Schema):
    """
    Convert Pulire schema to SQL insert
    """
    query = f"INSERT INTO `{table}` SET"
    for col in schema.columns():
        query += f" `{col}` = :{col},"
    query = query[:-1]
    query += " ON DUPLICATE KEY UPDATE"
    for col in schema.columns():
        query += f" `{col}` = COALESCE(VALUES(`{col}`),`{col}`),"
    query = query[:-1]
    return query

default_hourly_validators = {
    "temp": [validators.minimum(-100), validators.maximum(65)],
    "prcp": [validators.minimum(0), validators.maximum(350)],
    "snow": [validators.minimum(0), validators.maximum(11000)],
    "wdir": [validators.minimum(0), validators.maximum(360)],
    "wspd": [validators.minimum(0), validators.maximum(250)],
    "wpgt": [
        validators.minimum(0),
        validators.maximum(500),
        validators.greater("wspd"),
    ],
    "tsun": [validators.minimum(0), validators.maximum(60)],
    "srad": [validators.minimum(0), validators.maximum(1368)],
    "pres": [validators.minimum(850), validators.maximum(1090)],
    "rhum": [validators.minimum(0), validators.maximum(100)],
    "cldc": [validators.minimum(0), validators.maximum(100)],
    "vsby": [validators.minimum(0), validators.maximum(9999)],
    "coco": [validators.minimum(1), validators.maximum(27)],
}

default_daily_validators = {
    "tavg": [validators.minimum(-100), validators.maximum(65)],
    "tmin": [validators.minimum(-100), validators.maximum(65)],
    "tmax": [validators.minimum(-100), validators.maximum(65)],
    "prcp": [validators.minimum(0), validators.maximum(350)],
    "snow": [validators.minimum(0), validators.maximum(11000)],
    "wdir": [validators.minimum(0), validators.maximum(360)],
    "wspd": [validators.minimum(0), validators.maximum(250)],
    "wpgt": [
        validators.minimum(0),
        validators.maximum(500),
        validators.greater("wspd"),
    ],
    "tsun": [validators.minimum(0), validators.maximum(60)],
    "srad": [validators.minimum(0), validators.maximum(1368)],
    "pres": [validators.minimum(850), validators.maximum(1090)],
    "rhum": [validators.minimum(0), validators.maximum(100)],
    "cldc": [validators.minimum(0), validators.maximum(100)],
}

hourly_model = {
    "table": "hourly_model",
    "schema": Schema(
        {
            "temp": default_hourly_validators["temp"],
            "prcp": default_hourly_validators["prcp"],
            "snow": default_hourly_validators["snow"],
            "wdir": default_hourly_validators["wdir"],
            "wspd": default_hourly_validators["wspd"],
            "wpgt": default_hourly_validators["wpgt"],
            "tsun": default_hourly_validators["tsun"],
            "srad": default_hourly_validators["srad"],
            "pres": default_hourly_validators["pres"],
            "rhum": default_hourly_validators["rhum"],
            "cldc": default_hourly_validators["cldc"],
            "vsby": default_hourly_validators["vsby"],
            "coco": default_hourly_validators["coco"],
        }
    )
}

hourly_synop = {
    "table": "hourly_synop",                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           ",
    "schema": Schema(
        {
            "temp": default_hourly_validators["temp"],
            "prcp": default_hourly_validators["prcp"],
            "snow": default_hourly_validators["snow"],
            "wdir": default_hourly_validators["wdir"],
            "wspd": default_hourly_validators["wspd"],
            "wpgt": default_hourly_validators["wpgt"],
            "tsun": default_hourly_validators["tsun"],
            "srad": default_hourly_validators["srad"],
            "pres": default_hourly_validators["pres"],
            "rhum": default_hourly_validators["rhum"],
            "cldc": default_hourly_validators["cldc"],
            "vsby": default_hourly_validators["vsby"],
            "coco": default_hourly_validators["coco"],
        }
    )
}

hourly_national = {
    "table": "hourly_national",                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    ",
    "schema": Schema(
        {
            "temp": default_hourly_validators["temp"],
            "prcp": default_hourly_validators["prcp"],
            "snow": default_hourly_validators["snow"],
            "wdir": default_hourly_validators["wdir"],
            "wspd": default_hourly_validators["wspd"],
            "tsun": default_hourly_validators["tsun"],
            "srad": default_hourly_validators["srad"],
            "pres": default_hourly_validators["pres"],
            "rhum": default_hourly_validators["rhum"],
            "cldc": default_hourly_validators["cldc"],
            "vsby": default_hourly_validators["vsby"],
            "coco": default_hourly_validators["coco"],
        }
    )
}

hourly_gloabl = {
    "table": "hourly_isd",                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    ",
    "schema": Schema(
        {
            "temp": default_hourly_validators["temp"],
            "prcp": default_hourly_validators["prcp"],
            "wdir": default_hourly_validators["wdir"],
            "wspd": default_hourly_validators["wspd"],
            "pres": default_hourly_validators["pres"],
            "rhum": default_hourly_validators["rhum"],
            "cldc": default_hourly_validators["cldc"],
        }
    )
}

hourly_metar = {
    "table": "hourly_metar",                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    ",
    "schema": Schema(
        {
            "temp": default_hourly_validators["temp"],
            "wdir": default_hourly_validators["wdir"],
            "wspd": default_hourly_validators["wspd"],
            "pres": default_hourly_validators["pres"],
            "rhum": default_hourly_validators["rhum"],
            "cldc": default_hourly_validators["cldc"],
            "vsby": default_hourly_validators["vsby"],
            "coco": default_hourly_validators["coco"],
        }
    )
}

daily_national = {
    "table": "daily_national",
    "schema": Schema(
        {
            "tavg": default_daily_validators["tavg"],
            "tmin": default_daily_validators["tmin"],
            "tmax": default_daily_validators["tmax"],
            "prcp": default_daily_validators["prcp"],
            "snow": default_daily_validators["snow"],
            "wspd": default_daily_validators["wspd"],
            "wpgt": default_daily_validators["wpgt"],
            "tsun": default_daily_validators["tsun"],
            "srad": default_daily_validators["srad"],
            "pres": default_daily_validators["pres"],
            "rhum": default_daily_validators["rhum"],
            "cldc": default_daily_validators["cldc"],
        }
    )
}

daily_gloabl = {
    "table": "daily_ghcn",
    "schema": Schema(
        {
            "tavg": default_daily_validators["tavg"],
            "tmin": default_daily_validators["tmin"],
            "tmax": default_daily_validators["tmax"],
            "prcp": default_daily_validators["prcp"],
            "snow": default_daily_validators["snow"],
            "wdir": default_daily_validators["wdir"],
            "wspd": default_daily_validators["wspd"],
            "wpgt": default_daily_validators["wpgt"],
            "tsun": default_daily_validators["tsun"],
            "cldc": default_daily_validators["cldc"],
        }
    )
}