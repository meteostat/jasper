SELECT date(min(`time`)) as `date`,
       date_format(min(`time`), '%H') as `hour`,
        `temp`,
        `rhum`,
        `prcp`,
        NULL AS `snow`,
        `snow` AS `snwd`,
        `wdir`,
        `wspd`,
        `wpgt`,
        `pres`,
        `tsun`,
        `srad` AS `sghi`,
        NULL AS `sdni`,
        NULL AS `sdhi`,
        `cldc`,
        `vsby`,
        `coco`
FROM   `hourly_synop`
WHERE  `station` = :station
GROUP BY date_format(`time`, '%Y %m %d %H')
ORDER BY `time`