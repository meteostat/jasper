SELECT date_format(min(`time`), '%Y') as `year`,
       date_format(min(`time`), '%c') as `month`,
       date_format(min(`time`), '%d') as `day`,
       date_format(min(`time`), '%k') as `hour`,
        `temp`,
        `rhum`,
        `prcp`,
        `wdir`,
        `wspd`,
        `wpgt`,
        `pres`,
        `tsun`,
        `cldc`,
        `coco`
FROM   `hourly_model`
WHERE  `station` = :station
GROUP BY date_format(`time`, '%Y %m %d %H')
ORDER BY `time`