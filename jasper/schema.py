"""
Meteostat database schemas

The code is licensed under the MIT license.
"""

from jasper import validation


hourly_model = {
    "template": {
        "temp": None,
        "prcp": None,
        "wspd": None,
        "wpgt": None,
        "wdir": None,
        "tsun": None,
        "srad": None,
        "pres": None,
        "rhum": None,
        "cldc": None,
        "vsby": None,
        "coco": None,
    },
    "validation": {
        "temp": validation.temp,
        "prcp": validation.prcp_hourly,
        "wspd": validation.wspd,
        "wpgt": validation.wpgt,
        "wdir": validation.wdir,
        "tsun": validation.tsun_hourly,
        "srad": validation.srad,
        "pres": validation.pres,
        "rhum": validation.rhum,
        "cldc": validation.cldc,
        "vsby": validation.vsby,
        "coco": validation.coco,
    },
    "import_query": """
        INSERT INTO `hourly_model`
        SET
            `station` = :station,
            `time` = :time,
            `temp` = :temp,
            `prcp` = :prcp,
            `wspd` = :wspd,
            `wpgt` = :wpgt,
            `wdir` = :wdir,
            `tsun` = :tsun,
            `srad` = :srad,
            `pres` = :pres,
            `rhum` = :rhum,
            `cldc` = :cldc,
            `vsby` = :vsby,
            `coco` = :coco
        ON DUPLICATE KEY UPDATE
            `temp` = COALESCE(VALUES(`temp`),`temp`),
            `prcp` = COALESCE(VALUES(`prcp`),`prcp`),
            `wspd` = COALESCE(VALUES(`wspd`),`wspd`),
            `wpgt` = COALESCE(VALUES(`wpgt`),`wpgt`),
            `wdir` = COALESCE(VALUES(`wdir`),`wdir`),
            `tsun` = COALESCE(VALUES(`tsun`),`tsun`),
            `srad` = COALESCE(VALUES(`srad`),`srad`),
            `pres` = COALESCE(VALUES(`pres`),`pres`),
            `rhum` = COALESCE(VALUES(`rhum`),`rhum`),
            `cldc` = COALESCE(VALUES(`cldc`),`cldc`),
            `vsby` = COALESCE(VALUES(`vsby`),`vsby`),
            `coco` = COALESCE(VALUES(`coco`),`coco`)
    """,
}

hourly_synop = {
    "template": {
        "temp": None,
        "prcp": None,
        "snow": None,
        "wspd": None,
        "wpgt": None,
        "wdir": None,
        "tsun": None,
        "srad": None,
        "pres": None,
        "rhum": None,
        "cldc": None,
        "vsby": None,
        "coco": None,
    },
    "validation": {
        "temp": validation.temp,
        "prcp": validation.prcp_hourly,
        "snow": validation.snow,
        "wspd": validation.wspd,
        "wpgt": validation.wpgt,
        "wdir": validation.wdir,
        "tsun": validation.tsun_hourly,
        "srad": validation.srad,
        "pres": validation.pres,
        "rhum": validation.rhum,
        "cldc": validation.cldc,
        "vsby": validation.vsby,
        "coco": validation.coco,
    },
    "import_query": """
        INSERT INTO `hourly_synop`
        SET
            `station` = :station,
            `time` = :time,
            `temp` = :temp,
            `prcp` = :prcp,
            `snow` = :snow,
            `wspd` = :wspd,
            `wpgt` = :wpgt,
            `wdir` = :wdir,
            `tsun` = :tsun,
            `srad` = :srad,
            `pres` = :pres,
            `rhum` = :rhum,
            `cldc` = :cldc,
            `vsby` = :vsby,
            `coco` = :coco
        ON DUPLICATE KEY UPDATE
            `temp` = COALESCE(VALUES(`temp`),`temp`),
            `prcp` = COALESCE(VALUES(`prcp`),`prcp`),
            `snow` = COALESCE(VALUES(`snow`),`snow`),
            `wspd` = COALESCE(VALUES(`wspd`),`wspd`),
            `wpgt` = COALESCE(VALUES(`wpgt`),`wpgt`),
            `wdir` = COALESCE(VALUES(`wdir`),`wdir`),
            `tsun` = COALESCE(VALUES(`tsun`),`tsun`),
            `srad` = COALESCE(VALUES(`srad`),`srad`),
            `pres` = COALESCE(VALUES(`pres`),`pres`),
            `rhum` = COALESCE(VALUES(`rhum`),`rhum`),
            `cldc` = COALESCE(VALUES(`cldc`),`cldc`),
            `vsby` = COALESCE(VALUES(`vsby`),`vsby`),
            `coco` = COALESCE(VALUES(`coco`),`coco`)
    """,
}

hourly_national = {
    "template": {
        "temp": None,
        "prcp": None,
        "wspd": None,
        "wdir": None,
        "tsun": None,
        "srad": None,
        "pres": None,
        "rhum": None,
        "cldc": None,
        "vsby": None,
        "coco": None,
    },
    "validation": {
        "temp": validation.temp,
        "prcp": validation.prcp_hourly,
        "wspd": validation.wspd,
        "wdir": validation.wdir,
        "tsun": validation.tsun_hourly,
        "srad": validation.srad,
        "pres": validation.pres,
        "rhum": validation.rhum,
        "cldc": validation.cldc,
        "vsby": validation.vsby,
        "coco": validation.coco,
    },
    "import_query": """
        INSERT INTO `hourly_national`
        SET
            `station` = :station,
            `time` = :time,
            `temp` = :temp,
            `prcp` = :prcp,
            `wspd` = :wspd,
            `wdir` = :wdir,
            `tsun` = :tsun,
            `srad` = :srad,
            `pres` = :pres,
            `rhum` = :rhum,
            `cldc` = :cldc,
            `vsby` = :vsby,
            `coco` = :coco
        ON DUPLICATE KEY UPDATE
            `temp` = COALESCE(VALUES(`temp`),`temp`),
            `prcp` = COALESCE(VALUES(`prcp`),`prcp`),
            `wspd` = COALESCE(VALUES(`wspd`),`wspd`),
            `wdir` = COALESCE(VALUES(`wdir`),`wdir`),
            `tsun` = COALESCE(VALUES(`tsun`),`tsun`),
            `srad` = COALESCE(VALUES(`srad`),`srad`),
            `pres` = COALESCE(VALUES(`pres`),`pres`),
            `rhum` = COALESCE(VALUES(`rhum`),`rhum`),
            `cldc` = COALESCE(VALUES(`cldc`),`cldc`),
            `vsby` = COALESCE(VALUES(`vsby`),`vsby`),
            `coco` = COALESCE(VALUES(`coco`),`coco`)
    """,
}

hourly_global = {
    "template": {
        "temp": None,
        "prcp": None,
        "wspd": None,
        "wdir": None,
        "pres": None,
        "rhum": None,
        "cldc": None,
    },
    "validation": {
        "temp": validation.temp,
        "prcp": validation.prcp_hourly,
        "wspd": validation.wspd,
        "wdir": validation.wdir,
        "pres": validation.pres,
        "rhum": validation.rhum,
        "cldc": validation.cldc,
    },
    "import_query": """
        INSERT INTO `hourly_isd`
        SET
            `station` = :station,
            `time` = :time,
            `temp` = :temp,
            `prcp` = :prcp,
            `wspd` = :wspd,
            `wdir` = :wdir,
            `pres` = :pres,
            `rhum` = :rhum,
            `cldc` = :cldc
        ON DUPLICATE KEY UPDATE
            `temp` = COALESCE(VALUES(`temp`),`temp`),
            `prcp` = COALESCE(VALUES(`prcp`),`prcp`),
            `wspd` = COALESCE(VALUES(`wspd`),`wspd`),
            `wdir` = COALESCE(VALUES(`wdir`),`wdir`),
            `pres` = COALESCE(VALUES(`pres`),`pres`),
            `rhum` = COALESCE(VALUES(`rhum`),`rhum`),
            `cldc` = COALESCE(VALUES(`cldc`),`cldc`)
    """,
}

hourly_metar = {
    "template": {
        "temp": None,
        "wspd": None,
        "wdir": None,
        "pres": None,
        "rhum": None,
        "cldc": None,
        "vsby": None,
        "coco": None,
    },
    "validation": {
        "temp": validation.temp,
        "wspd": validation.wspd,
        "wdir": validation.wdir,
        "pres": validation.pres,
        "rhum": validation.rhum,
        "cldc": validation.cldc,
        "vsby": validation.vsby,
        "coco": validation.coco,
    },
    "import_query": """
        INSERT INTO `hourly_metar`
        SET
            `station` = :station,
            `time` = :time,
            `temp` = :temp,
            `wspd` = :wspd,
            `wdir` = :wdir,
            `pres` = :pres,
            `rhum` = :rhum,
            `cldc` = :cldc,
            `vsby` = :vsby,
            `coco` = :coco
        ON DUPLICATE KEY UPDATE
            `temp` = COALESCE(VALUES(`temp`),`temp`),
            `wspd` = COALESCE(VALUES(`wspd`),`wspd`),
            `wdir` = COALESCE(VALUES(`wdir`),`wdir`),
            `pres` = COALESCE(VALUES(`pres`),`pres`),
            `rhum` = COALESCE(VALUES(`rhum`),`rhum`),
            `cldc` = COALESCE(VALUES(`cldc`),`cldc`),
            `vsby` = COALESCE(VALUES(`vsby`),`vsby`),
            `coco` = COALESCE(VALUES(`coco`),`coco`)
    """,
}

daily_national = {
    "template": {
        "tavg": None,
        "tmin": None,
        "tmax": None,
        "prcp": None,
        "snow": None,
        "wspd": None,
        "wpgt": None,
        "tsun": None,
        "srad": None,
        "pres": None,
        "rhum": None,
        "cldc": None,
    },
    "validation": {
        "tavg": validation.temp,
        "tmin": validation.temp,
        "tmax": validation.temp,
        "prcp": validation.prcp_daily,
        "snow": validation.snow,
        "wspd": validation.wspd,
        "wpgt": validation.wpgt,
        "tsun": validation.tsun_daily,
        "srad": validation.srad,
        "pres": validation.pres,
        "rhum": validation.rhum,
        "cldc": validation.cldc,
    },
    "import_query": """
        INSERT INTO `daily_national`
        SET
            `station` = :station,
            `date` = :time,
            `tavg` = :tavg,
            `tmin` = :tmin,
            `tmax` = :tmax,
            `prcp` = :prcp,
            `snow` = :snow,
            `wspd` = :wspd,
            `wpgt` = :wpgt,
            `tsun` = :tsun,
            `srad` = :srad,
            `pres` = :pres,
            `rhum` = :rhum,
            `cldc` = :cldc
        ON DUPLICATE KEY UPDATE
            `tavg` = COALESCE(VALUES(`tavg`),`tavg`),
            `tmin` = COALESCE(VALUES(`tmin`),`tmin`),
            `tmax` = COALESCE(VALUES(`tmax`),`tmax`),
            `prcp` = COALESCE(VALUES(`prcp`),`prcp`),
            `snow` = COALESCE(VALUES(`snow`),`snow`),
            `wspd` = COALESCE(VALUES(`wspd`),`wspd`),
            `wpgt` = COALESCE(VALUES(`wpgt`),`wpgt`),
            `tsun` = COALESCE(VALUES(`tsun`),`tsun`),
            `srad` = COALESCE(VALUES(`srad`),`srad`),
            `pres` = COALESCE(VALUES(`pres`),`pres`),
            `rhum` = COALESCE(VALUES(`rhum`),`rhum`),
            `cldc` = COALESCE(VALUES(`cldc`),`cldc`)
    """,
}

daily_global = {
    "template": {
        "tavg": None,
        "tmin": None,
        "tmax": None,
        "prcp": None,
        "snow": None,
        "wspd": None,
        "wpgt": None,
        "wdir": None,
        "tsun": None,
        "cldc": None,
    },
    "validation": {
        "tavg": validation.temp,
        "tmin": validation.temp,
        "tmax": validation.temp,
        "prcp": validation.prcp_daily,
        "snow": validation.snow,
        "wspd": validation.wspd,
        "wpgt": validation.wpgt,
        "wdir": validation.wdir,
        "tsun": validation.tsun_daily,
        "cldc": validation.cldc,
    },
    "import_query": """
        INSERT INTO `daily_ghcn`
        SET
            `station` = :station,
            `date` = :time,
            `tavg` = :tavg,
            `tmin` = :tmin,
            `tmax` = :tmax,
            `prcp` = :prcp,
            `snow` = :snow,
            `wspd` = :wspd,
            `wpgt` = :wpgt,
            `wdir` = :wdir,
            `tsun` = :tsun,
            `cldc` = :cldc
        ON DUPLICATE KEY UPDATE
            `tavg` = COALESCE(VALUES(`tavg`),`tavg`),
            `tmin` = COALESCE(VALUES(`tmin`),`tmin`),
            `tmax` = COALESCE(VALUES(`tmax`),`tmax`),
            `prcp` = COALESCE(VALUES(`prcp`),`prcp`),
            `snow` = COALESCE(VALUES(`snow`),`snow`),
            `wspd` = COALESCE(VALUES(`wspd`),`wspd`),
            `wpgt` = COALESCE(VALUES(`wpgt`),`wpgt`),
            `wdir` = COALESCE(VALUES(`wdir`),`wdir`),
            `tsun` = COALESCE(VALUES(`tsun`),`tsun`),
            `cldc` = COALESCE(VALUES(`cldc`),`cldc`)
    """,
}

monthly_global = {
    "template": {
        "tavg": None,
        "tmin": None,
        "tmax": None,
        "prcp": None,
        "pres": None,
        "tsun": None,
    },
    "validation": {
        "tavg": validation.temp,
        "tmin": validation.temp,
        "tmax": validation.temp,
        "prcp": validation.prcp_monthly,
        "pres": validation.pres,
        "tsun": validation.tsun_monthly,
    },
    "import_query": """
        INSERT INTO `monthly_global`
        SET
            `station` = :station,
            `year` = :year,
            `month` = :month,
            `tavg` = :tavg,
            `tmin` = :tmin,
            `tmax` = :tmax,
            `prcp` = :prcp,
            `pres` = :pres,
            `tsun` = :tsun
        ON DUPLICATE KEY UPDATE
            `tavg` = COALESCE(VALUES(`tavg`),`tavg`),
            `tmin` = COALESCE(VALUES(`tmin`),`tmin`),
            `tmax` = COALESCE(VALUES(`tmax`),`tmax`),
            `prcp` = COALESCE(VALUES(`prcp`),`prcp`),
            `pres` = COALESCE(VALUES(`pres`),`pres`),
            `tsun` = COALESCE(VALUES(`tsun`),`tsun`)
    """,
}

normals_global = {
    "template": {
        "tavg": None,
        "tmin": None,
        "tmax": None,
        "prcp": None,
        "tsun": None,
        "pres": None,
    },
    "validation": {
        "tavg": validation.temp,
        "tmin": validation.temp,
        "tmax": validation.temp,
        "prcp": validation.prcp_monthly,
        "tsun": validation.tsun_monthly,
        "pres": validation.pres,
    },
    "import_query": """
        INSERT IGNORE INTO
            `normals_global`
        SET
            `station` = (SELECT `id` FROM `stations` WHERE `wmo` = :station),
            `start` = :start,
            `end` = :end,
            `month` = :month,
            `tavg` = :tavg,
            `tmin` = :tmin,
            `tmax` = :tmax,
            `prcp` = :prcp,
            `tsun` = :tsun,
            `pres` = :pres
        ON DUPLICATE KEY UPDATE
            `tavg` = COALESCE(VALUES(`tavg`),`tavg`),
            `tmin` = COALESCE(VALUES(`tmin`),`tmin`),
            `tmax` = COALESCE(VALUES(`tmax`),`tmax`),
            `prcp` = COALESCE(VALUES(`prcp`),`prcp`),
            `tsun` = COALESCE(VALUES(`tsun`),`tsun`),
            `pres` = COALESCE(VALUES(`pres`),`pres`)
    """,
}
