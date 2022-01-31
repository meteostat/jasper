SET STATEMENT
  max_statement_time = 90
FOR
SELECT
  DATE(MIN(`time`)) AS `date`,
  DATE_FORMAT(MIN(`time`), '%H') AS `hour`,
  SUBSTRING_INDEX(GROUP_CONCAT(`temp` ORDER BY `priority` ASC), ',', 1) AS `temp`,
  SUBSTRING_INDEX(GROUP_CONCAT(`dwpt` ORDER BY `priority` ASC), ',', 1) AS `dwpt`,
  SUBSTRING_INDEX(GROUP_CONCAT(`rhum` ORDER BY `priority` ASC), ',', 1) AS `rhum`,
  SUBSTRING_INDEX(GROUP_CONCAT(`prcp` ORDER BY `priority` ASC), ',', 1) AS `prcp`,
  SUBSTRING_INDEX(GROUP_CONCAT(`snow` ORDER BY `priority` ASC), ',', 1) AS `snow`,
  SUBSTRING_INDEX(GROUP_CONCAT(`wdir` ORDER BY `priority` ASC), ',', 1) AS `wdir`,
  SUBSTRING_INDEX(GROUP_CONCAT(`wspd` ORDER BY `priority` ASC), ',', 1) AS `wspd`,
  SUBSTRING_INDEX(GROUP_CONCAT(`wpgt` ORDER BY `priority` ASC), ',', 1) AS `wpgt`,
  SUBSTRING_INDEX(GROUP_CONCAT(`pres` ORDER BY `priority` ASC), ',', 1) AS `pres`,
  SUBSTRING_INDEX(GROUP_CONCAT(`tsun` ORDER BY `priority` ASC), ',', 1) AS `tsun`,
  SUBSTRING_INDEX(GROUP_CONCAT(`coco` ORDER BY `priority` ASC), ',', 1) AS `coco`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`temp`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `temp_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`dwpt`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `dwpt_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`rhum`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `rhum_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`prcp`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `prcp_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`snow`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `snow_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`wdir`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `wdir_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`wspd`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `wspd_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`wpgt`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `wpgt_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`pres`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `pres_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`tsun`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `tsun_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`coco`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `coco_flag`
FROM (
  (
    SELECT
      `time`,
      `temp`,
      ROUND((243.04*(LN(`rhum`/100)+((17.625*`temp`)/(243.04+`temp`)))/(17.625-LN(`rhum`/100)-((17.625*`temp`)/(243.04+`temp`)))), 1) AS `dwpt`,
      `rhum`,
      `prcp`,
      NULL AS `snow`,
      `wdir`,
      `wspd`,
      NULL AS `wpgt`,
      `pres`,
      `tsun`,
      NULL AS `coco`,
      'A' AS `priority`
    FROM
      `hourly_national`
    WHERE
      `station` = :station AND
      `time` BETWEEN :start_datetime AND :end_datetime
  ) UNION ALL
  (
    SELECT
      `time`,
      `temp`,
      ROUND(
        (
          243.04 * (
            LN(`rhum` / 100) +
            (
              (17.625 * `temp`) / (243.04 + `temp`)
            )
          ) / (
            17.625 - LN(`rhum` / 100) -
            (
              (17.625 * `temp`) / (243.04 + `temp`)
            )
          )
        ),
        1
      ) AS `dwpt`,
      `rhum`,
      `prcp`,
      NULL AS `snow`,
      `wdir`,
      `wspd`,
      NULL AS `wpgt`,
      `pres`,
      NULL AS `tsun`,
      NULL AS `coco`,
      'B' AS `priority`
    FROM
      `hourly_isd`
    WHERE
      `station` = :station AND
      `time` BETWEEN :start_datetime AND :end_datetime
  ) UNION ALL
  (
    SELECT
      `time`,
      `temp`,
      ROUND(
        (
          243.04 * (
            LN(`rhum` / 100) +
            (
              (17.625 * `temp`) / (243.04 + `temp`)
            )
          ) / (
            17.625 - LN(`rhum` / 100) -
            (
              (17.625 * `temp`) / (243.04 + `temp`)
            )
          )
        ),
        1
      ) AS `dwpt`,
      `rhum`,
      `prcp`,
      `snow`,
      `wdir`,
      `wspd`,
      `wpgt`,
      `pres`,
      `tsun`,
      `coco`,
      'C' AS `priority`
    FROM
      `hourly_synop`
    WHERE
      `station` = :station AND
      `time` BETWEEN :start_datetime AND :end_datetime
  ) UNION ALL
  (
    SELECT
      `time`,
      `temp`,
      ROUND(
        (
          243.04 * (
            LN(`rhum` / 100) +
            (
              (17.625 * `temp`) / (243.04 + `temp`)
            )
          ) / (
            17.625 - LN(`rhum` / 100) -
            (
              (17.625 * `temp`) / (243.04 + `temp`)
            )
          )
        ),
        1
      ) AS `dwpt`,
      `rhum`,
      NULL AS `prcp`,
      NULL AS `snow`,
      `wdir`,
      `wspd`,
      NULL AS `wpgt`,
      `pres`,
      NULL AS `tsun`,
      `coco`,
      'D' AS `priority`
    FROM
      `hourly_metar`
    WHERE
      `station` = :station AND
      `time` BETWEEN :start_datetime AND :end_datetime
  ) UNION ALL
  (
    SELECT
      `time`,
      `temp`,
      ROUND(
        (
          243.04 * (
            LN(`rhum` / 100) +
            (
              (17.625 * `temp`) / (243.04 + `temp`)
            )
          ) / (
            17.625 - LN(`rhum` / 100) -
            (
              (17.625 * `temp`) / (243.04 + `temp`)
            )
          )
        ),
        1
      ) AS `dwpt`,
      `rhum`,
      `prcp`,
      NULL AS `snow`,
      `wdir`,
      `wspd`,
      `wpgt`,
      `pres`,
      NULL AS `tsun`,
      `coco`,
      'E' AS `priority`
    FROM
      `hourly_model`
    WHERE
      `station` = :station AND
      `time` BETWEEN :start_datetime AND :end_datetime
  )
) AS `hourly_derived`
WHERE
  `time` <= DATE_ADD(NOW(), INTERVAL 10 DAY)
GROUP BY
  DATE_FORMAT(`time`, '%Y %m %d %H')
ORDER BY
  `time`
