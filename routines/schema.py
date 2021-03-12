from routines import validation

hourly_model = {
    'index': ['station', 'time'],
    'template': {
        'temp': None,
        'rhum': None,
        'prcp': None,
        'wspd': None,
        'wpgt': None,
        'wdir': None,
        'pres': None
    },
    'validation': {
        'temp': validation.temp,
        'rhum': validation.rhum,
        'prcp': validation.prcp_hourly,
        'wspd': validation.wspd,
        'wpgt': validation.wpgt,
        'wdir': validation.wdir,
        'pres': validation.pres
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
            `pres` = :pres
        ON DUPLICATE KEY UPDATE
            `temp` = COALESCE(VALUES(`temp`),`temp`),
            `rhum` = COALESCE(VALUES(`rhum`),`rhum`),
            `prcp` = COALESCE(VALUES(`prcp`),`prcp`),
            `wspd` = COALESCE(VALUES(`wspd`),`wspd`),
            `wpgt` = COALESCE(VALUES(`wpgt`),`wpgt`),
            `wdir` = COALESCE(VALUES(`wdir`),`wdir`),
            `pres` = COALESCE(VALUES(`pres`),`pres`)
    """
}

hourly_synop = {
    'index': ['station', 'time'],
    'template': {
        'temp': None,
        'rhum': None,
        'prcp': None,
        'wspd': None,
        'wpgt': None,
        'wdir': None,
        'pres': None,
        'tsun': None
    },
    'validation': {
        'temp': validation.temp,
        'rhum': validation.rhum,
        'prcp': validation.prcp_hourly,
        'wspd': validation.wspd,
        'wpgt': validation.wpgt,
        'wdir': validation.wdir,
        'pres': validation.pres,
        'tsun': validation.tsun_hourly
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
            `tsun` = :tsun
        ON DUPLICATE KEY UPDATE
            `temp` = COALESCE(VALUES(`temp`),`temp`),
            `rhum` = COALESCE(VALUES(`rhum`),`rhum`),
            `prcp` = COALESCE(VALUES(`prcp`),`prcp`),
            `wspd` = COALESCE(VALUES(`wspd`),`wspd`),
            `wpgt` = COALESCE(VALUES(`wpgt`),`wpgt`),
            `wdir` = COALESCE(VALUES(`wdir`),`wdir`),
            `pres` = COALESCE(VALUES(`pres`),`pres`),
            `tsun` = COALESCE(VALUES(`tsun`),`tsun`)
    """
}
