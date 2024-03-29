SELECT date_format(min(`time`), '%Y') as `year`,
       date_format(min(`time`), '%c') as `month`,
       date_format(min(`time`), '%e') as `day`,
       date_format(min(`time`), '%k') as `hour`,
        `temp`,
        `rhum`,
        `wdir`,
        `wspd`,
        `pres`,
        `cldc`,
        `coco`
FROM   `hourly_metar`
WHERE  `station` = :station
GROUP BY date_format(`time`, '%Y %m %d %H')
ORDER BY `time`