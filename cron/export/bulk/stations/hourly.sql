SET STATEMENT
  max_statement_time = 90
FOR
SELECT
  DATE(MIN(`time`)) AS `date`,
  DATE_FORMAT(MIN(`time`), '%H') AS `hour`,
  CAST(SUBSTRING_INDEX(GROUP_CONCAT(`temp` ORDER BY `flag` ASC), ',', 1) AS DECIMAL(3, 1)) AS `temp`,
  CAST(SUBSTRING_INDEX(GROUP_CONCAT(`dwpt` ORDER BY `flag` ASC), ',', 1) AS DECIMAL(3, 1)) AS `dwpt`,
  CAST(SUBSTRING_INDEX(GROUP_CONCAT(`rhum` ORDER BY `flag` ASC), ',', 1) AS INT) AS `rhum`,
  CAST(SUBSTRING_INDEX(GROUP_CONCAT(`prcp` ORDER BY `flag` ASC), ',', 1) AS DECIMAL(4, 1)) AS `prcp`,
  CAST(SUBSTRING_INDEX(GROUP_CONCAT(`snow` ORDER BY `flag` ASC), ',', 1) AS INT) AS `snow`,
  CAST(SUBSTRING_INDEX(GROUP_CONCAT(`wdir` ORDER BY `flag` ASC), ',', 1) AS INT) AS `wdir`,
  CAST(SUBSTRING_INDEX(GROUP_CONCAT(`wspd` ORDER BY `flag` ASC), ',', 1) AS DECIMAL(4, 1)) AS `wspd`,
  CAST(SUBSTRING_INDEX(GROUP_CONCAT(`wpgt` ORDER BY `flag` ASC), ',', 1) AS DECIMAL(4, 1)) AS `wpgt`,
  CAST(SUBSTRING_INDEX(GROUP_CONCAT(`pres` ORDER BY `flag` ASC), ',', 1) AS DECIMAL(5, 1)) AS `pres`,
  CAST(SUBSTRING_INDEX(GROUP_CONCAT(`tsun` ORDER BY `flag` ASC), ',', 1) AS INT) AS `tsun`,
  CAST(SUBSTRING_INDEX(GROUP_CONCAT(`coco` ORDER BY `flag` ASC), ',', 1) AS INT) AS `coco`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`temp`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `temp_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`dwpt`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `dwpt_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`rhum`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `rhum_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`prcp`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `prcp_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`snow`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `snow_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`wdir`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `wdir_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`wspd`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `wspd_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`wpgt`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `wpgt_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`pres`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `pres_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`tsun`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `tsun_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`coco`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `coco_flag`
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
      'A' AS `flag`
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
      'B' AS `flag`
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
      'C' AS `flag`
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
      'D' AS `flag`
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
      'E' AS `flag`
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
