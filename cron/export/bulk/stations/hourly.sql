set statement max_statement_time = 90 for
SELECT date(min(`time`)) as `date`,
       date_format(min(`time`), '%H') as `hour`,
       cast(substring_index(group_concat(`temp` ORDER BY `flag` asc), ',', 1) as decimal(3, 1)) as `temp`,
       cast(substring_index(group_concat(`dwpt` ORDER BY `flag` asc), ',', 1) as decimal(3, 1)) as `dwpt`,
       cast(substring_index(group_concat(`rhum` ORDER BY `flag` asc), ',', 1) as int) as `rhum`,
       cast(substring_index(group_concat(`prcp` ORDER BY `flag` asc), ',', 1) as decimal(4, 1)) as `prcp`,
       cast(substring_index(group_concat(`snow` ORDER BY `flag` asc), ',', 1) as int) as `snow`,
       cast(substring_index(group_concat(`wdir` ORDER BY `flag` asc), ',', 1) as int) as `wdir`,
       cast(substring_index(group_concat(`wspd` ORDER BY `flag` asc), ',', 1) as decimal(4, 1)) as `wspd`,
       cast(substring_index(group_concat(`wpgt` ORDER BY `flag` asc), ',', 1) as decimal(4, 1)) as `wpgt`,
       cast(substring_index(group_concat(`pres` ORDER BY `flag` asc), ',', 1) as decimal(5, 1)) as `pres`,
       cast(substring_index(group_concat(`tsun` ORDER BY `flag` asc), ',', 1) as int) as `tsun`,
       cast(substring_index(group_concat(`coco` ORDER BY `flag` asc), ',', 1) as int) as `coco`,
       substr(substring_index(group_concat(concat(`temp`, ':', `flag`) ORDER BY `flag` asc), ',', 1),
              -1,
              1) as `temp_flag`,
       substr(substring_index(group_concat(concat(`dwpt`, ':', `flag`) ORDER BY `flag` asc), ',', 1),
              -1,
              1) as `dwpt_flag`,
       substr(substring_index(group_concat(concat(`rhum`, ':', `flag`) ORDER BY `flag` asc), ',', 1),
              -1,
              1) as `rhum_flag`,
       substr(substring_index(group_concat(concat(`prcp`, ':', `flag`) ORDER BY `flag` asc), ',', 1),
              -1,
              1) as `prcp_flag`,
       substr(substring_index(group_concat(concat(`snow`, ':', `flag`) ORDER BY `flag` asc), ',', 1),
              -1,
              1) as `snow_flag`,
       substr(substring_index(group_concat(concat(`wdir`, ':', `flag`) ORDER BY `flag` asc), ',', 1),
              -1,
              1) as `wdir_flag`,
       substr(substring_index(group_concat(concat(`wspd`, ':', `flag`) ORDER BY `flag` asc), ',', 1),
              -1,
              1) as `wspd_flag`,
       substr(substring_index(group_concat(concat(`wpgt`, ':', `flag`) ORDER BY `flag` asc), ',', 1),
              -1,
              1) as `wpgt_flag`,
       substr(substring_index(group_concat(concat(`pres`, ':', `flag`) ORDER BY `flag` asc), ',', 1),
              -1,
              1) as `pres_flag`,
       substr(substring_index(group_concat(concat(`tsun`, ':', `flag`) ORDER BY `flag` asc), ',', 1),
              -1,
              1) as `tsun_flag`,
       substr(substring_index(group_concat(concat(`coco`, ':', `flag`) ORDER BY `flag` asc), ',', 1),
              -1,
              1) as `coco_flag`
FROM   ((SELECT `time`,
                `temp`,
                round((243.04*(ln(`rhum`/100)+((17.625*`temp`)/(243.04+`temp`)))/(17.625-ln(`rhum`/100)-((17.625*`temp`)/(243.04+`temp`)))),
                      1) as `dwpt`,
                `rhum`,
                `prcp`,
                null as `snow`,
                `wdir`,
                `wspd`,
                null as `wpgt`,
                `pres`,
                `tsun`,
                null as `coco`,
                'A' as `flag`
         FROM   `hourly_national`
         WHERE  `station` = :station
            and `time` between :start_datetime
            and :end_datetime)
UNION all (SELECT `time`,
                  `temp`,
                  round((243.04 * (ln(`rhum` / 100) + ((17.625 * `temp`) / (243.04 + `temp`))) / (17.625 - ln(`rhum` / 100) - ((17.625 * `temp`) / (243.04 + `temp`)))),
                        1) as `dwpt`,
                  `rhum`,
                  `prcp`,
                  null as `snow`,
                  `wdir`,
                  `wspd`,
                  null as `wpgt`,
                  `pres`,
                  null as `tsun`,
                  null as `coco`,
                  'B' as `flag`
           FROM   `hourly_isd`
           WHERE  `station` = :station
              and `time` between :start_datetime
              and :end_datetime)
UNION all (SELECT `time`,
                  `temp`,
                  round((243.04 * (ln(`rhum` / 100) + ((17.625 * `temp`) / (243.04 + `temp`))) / (17.625 - ln(`rhum` / 100) - ((17.625 * `temp`) / (243.04 + `temp`)))),
                        1) as `dwpt`,
                  `rhum`,
                  `prcp`,
                  `snow`,
                  `wdir`,
                  `wspd`,
                  `wpgt`,
                  `pres`,
                  `tsun`,
                  `coco`,
                  'C' as `flag`
           FROM   `hourly_synop`
           WHERE  `station` = :station
              and `time` between :start_datetime
              and :end_datetime)
UNION all (SELECT `time`,
                  `temp`,
                  round((243.04 * (ln(`rhum` / 100) + ((17.625 * `temp`) / (243.04 + `temp`))) / (17.625 - ln(`rhum` / 100) - ((17.625 * `temp`) / (243.04 + `temp`)))),
                        1) as `dwpt`,
                  `rhum`,
                  null as `prcp`,
                  null as `snow`,
                  `wdir`,
                  `wspd`,
                  null as `wpgt`,
                  `pres`,
                  null as `tsun`,
                  `coco`,
                  'D' as `flag`
           FROM   `hourly_metar`
           WHERE  `station` = :station
              and `time` between :start_datetime
              and :end_datetime)
UNION all (SELECT `time`,
                  `temp`,
                  round((243.04 * (ln(`rhum` / 100) + ((17.625 * `temp`) / (243.04 + `temp`))) / (17.625 - ln(`rhum` / 100) - ((17.625 * `temp`) / (243.04 + `temp`)))),
                        1) as `dwpt`,
                  `rhum`,
                  `prcp`,
                  null as `snow`,
                  `wdir`,
                  `wspd`,
                  `wpgt`,
                  `pres`,
                  null as `tsun`,
                  `coco`,
                  'E' as `flag`
           FROM   `hourly_model`
           WHERE  `station` = :station
              and `time` between :start_datetime
              and :end_datetime)) as `hourly_derived`
WHERE  `time` <= date_add(now(), interval 10 day)
GROUP BY date_format(`time`, '%Y %m %d %H')
ORDER BY `time`
