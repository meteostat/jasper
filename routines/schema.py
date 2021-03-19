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
