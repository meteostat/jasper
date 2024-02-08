SELECT date_format(min(`time`), '%Y') as `year`,
       date_format(min(`time`), '%c') as `month`,
       date_format(min(`time`), '%e') as `day`,
       date_format(min(`time`), '%k') as `hour`,
        `temp`,
        `rhum`,
        `prcp`,
        `snow` AS `snwd`,
        `wdir`,
        `wspd`,
        `wpgt`,
        `pres`,
        `tsun`,
        `cldc`,
        `coco`
FROM   `hourly_synop`
WHERE  `station` = :station
GROUP BY date_format(`time`, '%Y %m %d %H')
ORDER BY `time`