SELECT date(min(`time`)) as `date`,
       date_format(min(`time`), '%H') as `hour`,
        `temp`,
        `rhum`,
        `prcp`,
        `wdir`,
        `wspd`,
        `wpgt`,
        `pres`,
        `tsun`,
        `srad` AS `sghi`,
        `cldc`,
        `vsby`,
        `coco`
FROM   `hourly_model`
WHERE  `station` = :station
GROUP BY date_format(`time`, '%Y %m %d %H')
ORDER BY `time`