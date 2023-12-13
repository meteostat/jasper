SELECT date(min(`time`)) as `date`,
       date_format(min(`time`), '%H') as `hour`,
        `temp`,
        `rhum`,
        `prcp`,
        `snow` AS `snwd`,
        `wdir`,
        `wspd`,
        `wpgt`,
        `pres`,
        `tsun`,
        `coco`
FROM   `hourly_synop`
WHERE  `station` = :station
GROUP BY date_format(`time`, '%Y %m %d %H')
ORDER BY `time`