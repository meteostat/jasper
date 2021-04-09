from routines import validation

hourly_model = {
    'template': {
        'temp': None,
        'rhum': None,
        'prcp': None,
        'wspd': None,
        'wpgt': None,
        'wdir': None,
        'pres': None,
        'coco': None
    },
    'validation': {
        'temp': validation.temp,
        'rhum': validation.rhum,
        'prcp': validation.prcp_hourly,
        'wspd': validation.wspd,
        'wpgt': validation.wpgt,
        'wdir': validation.wdir,
        'pres': validation.pres,
        'coco': validation.coco
    },
    'import_query': """
        INSERT INTO `hourly_model`
        SET
            `station` = :station,
            `time` = :time,
            `temp` = :temp,
            `rhum` = :rhum,
            `prcp` = :prcp,
            `wspd` = :wspd,
            `wpgt` = :wpgt,
            `wdir` = :wdir,
            `pres` = :pres,
            `coco` = :coco
        ON DUPLICATE KEY UPDATE
            `temp` = COALESCE(VALUES(`temp`),`temp`),
            `rhum` = COALESCE(VALUES(`rhum`),`rhum`),
            `prcp` = COALESCE(VALUES(`prcp`),`prcp`),
            `wspd` = COALESCE(VALUES(`wspd`),`wspd`),
            `wpgt` = COALESCE(VALUES(`wpgt`),`wpgt`),
            `wdir` = COALESCE(VALUES(`wdir`),`wdir`),
            `pres` = COALESCE(VALUES(`pres`),`pres`),
            `coco` = COALESCE(VALUES(`coco`),`coco`)
    """
}

hourly_synop = {
    'template': {
        'temp': None,
        'rhum': None,
        'prcp': None,
        'wspd': None,
        'wpgt': None,
        'wdir': None,
        'pres': None,
        'snow': None,
        'tsun': None,
        'coco': None
    },
    'validation': {
        'temp': validation.temp,
        'rhum': validation.rhum,
        'prcp': validation.prcp_hourly,
        'wspd': validation.wspd,
        'wpgt': validation.wpgt,
        'wdir': validation.wdir,
        'pres': validation.pres,
        'snow': validation.snow,
        'tsun': validation.tsun_hourly,
        'coco': validation.coco
    },
    'import_query': """
        INSERT INTO `hourly_synop`
        SET
            `station` = :station,
            `time` = :time,
            `temp` = :temp,
            `rhum` = :rhum,
            `prcp` = :prcp,
            `wspd` = :wspd,
            `wpgt` = :wpgt,
            `wdir` = :wdir,
            `pres` = :pres,
            `snow` = :snow,
            `tsun` = :tsun,
            `coco` = :coco
        ON DUPLICATE KEY UPDATE
            `temp` = COALESCE(VALUES(`temp`),`temp`),
            `rhum` = COALESCE(VALUES(`rhum`),`rhum`),
            `prcp` = COALESCE(VALUES(`prcp`),`prcp`),
            `wspd` = COALESCE(VALUES(`wspd`),`wspd`),
            `wpgt` = COALESCE(VALUES(`wpgt`),`wpgt`),
            `wdir` = COALESCE(VALUES(`wdir`),`wdir`),
            `pres` = COALESCE(VALUES(`pres`),`pres`),
            `snow` = COALESCE(VALUES(`snow`),`snow`),
            `tsun` = COALESCE(VALUES(`tsun`),`tsun`),
            `coco` = COALESCE(VALUES(`coco`),`coco`)
    """
}

hourly_national = {
    'template': {
        'temp': None,
        'rhum': None,
        'prcp': None,
        'wspd': None,
        'wdir': None,
        'pres': None,
        'tsun': None
    },
    'validation': {
        'temp': validation.temp,
        'rhum': validation.rhum,
        'prcp': validation.prcp_hourly,
        'wspd': validation.wspd,
        'wdir': validation.wdir,
        'pres': validation.pres,
        'tsun': validation.tsun_hourly
    },
    'import_query': """
        INSERT INTO `hourly_national`
        SET
            `station` = :station,
            `time` = :time,
            `temp` = :temp,
            `rhum` = :rhum,
            `prcp` = :prcp,
            `wspd` = :wspd,
            `wdir` = :wdir,
            `pres` = :pres,
            `tsun` = :tsun
        ON DUPLICATE KEY UPDATE
            `temp` = COALESCE(VALUES(`temp`),`temp`),
            `rhum` = COALESCE(VALUES(`rhum`),`rhum`),
            `prcp` = COALESCE(VALUES(`prcp`),`prcp`),
            `wspd` = COALESCE(VALUES(`wspd`),`wspd`),
            `wdir` = COALESCE(VALUES(`wdir`),`wdir`),
            `pres` = COALESCE(VALUES(`pres`),`pres`),
            `tsun` = COALESCE(VALUES(`tsun`),`tsun`)
    """
}

hourly_global = {
    'template': {
        'temp': None,
        'rhum': None,
        'prcp': None,
        'wspd': None,
        'wdir': None,
        'pres': None
    },
    'validation': {
        'temp': validation.temp,
        'rhum': validation.rhum,
        'prcp': validation.prcp_hourly,
        'wspd': validation.wspd,
        'wdir': validation.wdir,
        'pres': validation.pres
    },
    'import_query': """
        INSERT INTO `hourly_isd`
        SET
            `station` = :station,
            `time` = :time,
            `temp` = :temp,
            `rhum` = :rhum,
            `prcp` = :prcp,
            `wspd` = :wspd,
            `wdir` = :wdir,
            `pres` = :pres
        ON DUPLICATE KEY UPDATE
            `temp` = COALESCE(VALUES(`temp`),`temp`),
            `rhum` = COALESCE(VALUES(`rhum`),`rhum`),
            `prcp` = COALESCE(VALUES(`prcp`),`prcp`),
            `wspd` = COALESCE(VALUES(`wspd`),`wspd`),
            `wdir` = COALESCE(VALUES(`wdir`),`wdir`),
            `pres` = COALESCE(VALUES(`pres`),`pres`)
    """
}

hourly_metar = {
    'template': {
        'temp': None,
        'rhum': None,
        'wspd': None,
        'wdir': None,
        'pres': None,
        'coco': None
    },
    'validation': {
        'temp': validation.temp,
        'rhum': validation.rhum,
        'wspd': validation.wspd,
        'wdir': validation.wdir,
        'pres': validation.pres,
        'coco': validation.coco
    },
    'import_query': """
        INSERT INTO `hourly_metar`
        SET
            `station` = :station,
            `time` = :time,
            `temp` = :temp,
            `rhum` = :rhum,
            `wspd` = :wspd,
            `wdir` = :wdir,
            `pres` = :pres,
            `coco` = :coco
        ON DUPLICATE KEY UPDATE
            `temp` = COALESCE(VALUES(`temp`),`temp`),
            `rhum` = COALESCE(VALUES(`rhum`),`rhum`),
            `wspd` = COALESCE(VALUES(`wspd`),`wspd`),
            `wdir` = COALESCE(VALUES(`wdir`),`wdir`),
            `pres` = COALESCE(VALUES(`pres`),`pres`),
            `coco` = COALESCE(VALUES(`coco`),`coco`)
    """
}

daily_national = {
    'template': {
        'tavg': None,
        'tmin': None,
        'tmax': None,
        'rhum': None,
        'prcp': None,
        'wspd': None,
        'wpgt': None,
        'pres': None,
        'snow': None,
        'tsun': None
    },
    'validation': {
        'tavg': validation.temp,
        'tmin': validation.temp,
        'tmax': validation.temp,
        'prcp': validation.prcp_daily,
        'rhum': validation.rhum,
        'wspd': validation.wspd,
        'wpgt': validation.wpgt,
        'pres': validation.pres,
        'snow': validation.snow,
        'tsun': validation.tsun_daily
    },
    'import_query': """
        INSERT INTO `daily_national`
        SET
            `station` = :station,
            `date` = :time,
            `tavg` = :tavg,
            `tmin` = :tmin,
            `tmax` = :tmax,
            `rhum` = :rhum,
            `prcp` = :prcp,
            `wspd` = :wspd,
            `wpgt` = :wpgt,
            `pres` = :pres,
            `snow` = :snow,
            `tsun` = :tsun
        ON DUPLICATE KEY UPDATE
            `tavg` = COALESCE(VALUES(`tavg`),`tavg`),
            `tmin` = COALESCE(VALUES(`tmin`),`tmin`),
            `tmax` = COALESCE(VALUES(`tmax`),`tmax`),
            `rhum` = COALESCE(VALUES(`rhum`),`rhum`),
            `prcp` = COALESCE(VALUES(`prcp`),`prcp`),
            `wspd` = COALESCE(VALUES(`wspd`),`wspd`),
            `wpgt` = COALESCE(VALUES(`wpgt`),`wpgt`),
            `pres` = COALESCE(VALUES(`pres`),`pres`),
            `snow` = COALESCE(VALUES(`snow`),`snow`),
            `tsun` = COALESCE(VALUES(`tsun`),`tsun`)
    """
}

daily_global = {
    'template': {
        'tavg': None,
        'tmin': None,
        'tmax': None,
        'prcp': None,
        'wspd': None,
        'wpgt': None,
        'wdir': None,
        'snow': None,
        'tsun': None
    },
    'validation': {
        'tavg': validation.temp,
        'tmin': validation.temp,
        'tmax': validation.temp,
        'prcp': validation.prcp_daily,
        'wspd': validation.wspd,
        'wpgt': validation.wpgt,
        'wdir': validation.wdir,
        'snow': validation.snow,
        'tsun': validation.tsun_daily
    },
    'import_query': """
        INSERT INTO `daily_ghcn`
        SET
            `station` = :station,
            `date` = :time,
            `tavg` = :tavg,
            `tmin` = :tmin,
            `tmax` = :tmax,
            `prcp` = :prcp,
            `wspd` = :wspd,
            `wpgt` = :wpgt,
            `wdir` = :wdir,
            `snow` = :snow,
            `tsun` = :tsun
        ON DUPLICATE KEY UPDATE
            `tavg` = COALESCE(VALUES(`tavg`),`tavg`),
            `tmin` = COALESCE(VALUES(`tmin`),`tmin`),
            `tmax` = COALESCE(VALUES(`tmax`),`tmax`),
            `prcp` = COALESCE(VALUES(`prcp`),`prcp`),
            `wspd` = COALESCE(VALUES(`wspd`),`wspd`),
            `wpgt` = COALESCE(VALUES(`wpgt`),`wpgt`),
            `wdir` = COALESCE(VALUES(`wdir`),`wdir`),
            `snow` = COALESCE(VALUES(`snow`),`snow`),
            `tsun` = COALESCE(VALUES(`tsun`),`tsun`)
    """
}

monthly_global = {
    'template': {
        'tavg': None,
        'tmin': None,
        'tmax': None,
        'prcp': None,
        'pres': None,
        'tsun': None
    },
    'validation': {
        'tavg': validation.temp,
        'tmin': validation.temp,
        'tmax': validation.temp,
        'prcp': validation.prcp_monthly,
        'pres': validation.pres,
        'tsun': validation.tsun_monthly
    },
    'import_query': """
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
    """
}

normals_global = {
    'template': {
        'tavg': None,
        'tmin': None,
        'tmax': None,
        'prcp': None,
        'pres': None,
        'tsun': None
    },
    'validation': {
        'tavg': validation.temp,
        'tmin': validation.temp,
        'tmax': validation.temp,
        'prcp': validation.prcp_monthly,
        'pres': validation.pres,
        'tsun': validation.tsun_monthly
    },
    'import_query': """
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
            `pres` = :pres,
            `tsun` = :tsun
        ON DUPLICATE KEY UPDATE
            `tavg` = COALESCE(VALUES(`tavg`),`tavg`),
            `tmin` = COALESCE(VALUES(`tmin`),`tmin`),
            `tmax` = COALESCE(VALUES(`tmax`),`tmax`),
            `prcp` = COALESCE(VALUES(`prcp`),`prcp`),
            `pres` = COALESCE(VALUES(`pres`),`pres`),
            `tsun` = COALESCE(VALUES(`tsun`),`tsun`)
    """
}
