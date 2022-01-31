SET STATEMENT
  max_statement_time = 300
FOR
SELECT
  `date`,
  SUBSTRING_INDEX(GROUP_CONCAT(`tavg` ORDER BY `tavg_flag` ASC), ",", 1) AS `tavg`,
  SUBSTRING_INDEX(GROUP_CONCAT(`tmin` ORDER BY `tmin_flag` ASC), ",", 1) AS `tmin`,
  SUBSTRING_INDEX(GROUP_CONCAT(`tmax` ORDER BY `tmax_flag` ASC), ",", 1) AS `tmax`,
  SUBSTRING_INDEX(GROUP_CONCAT(`prcp` ORDER BY `prcp_flag` ASC), ",", 1) AS `prcp`,
  SUBSTRING_INDEX(GROUP_CONCAT(`snow` ORDER BY `snow_flag` ASC), ",", 1) AS `snow`,
  SUBSTRING_INDEX(GROUP_CONCAT(`wdir` ORDER BY `wdir_flag` ASC), ",", 1) AS `wdir`,
  SUBSTRING_INDEX(GROUP_CONCAT(`wspd` ORDER BY `wspd_flag` ASC), ",", 1) AS `wspd`,
  SUBSTRING_INDEX(GROUP_CONCAT(`wpgt` ORDER BY `wpgt_flag` ASC), ",", 1) AS `wpgt`,
  SUBSTRING_INDEX(GROUP_CONCAT(`pres` ORDER BY `pres_flag` ASC), ",", 1) AS `pres`,
  SUBSTRING_INDEX(GROUP_CONCAT(`tsun` ORDER BY `tsun_flag` ASC), ",", 1) AS `tsun`,
  IF(COUNT(`tavg`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`tavg_flag`), ',', 1)) AS `tavg_flag`,
  IF(COUNT(`tmin`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`tmin_flag`), ',', 1)) AS `tmin_flag`,
  IF(COUNT(`tmax`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`tmax_flag`), ',', 1)) AS `tmax_flag`,
  IF(COUNT(`prcp`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`prcp_flag`), ',', 1)) AS `prcp_flag`,
  IF(COUNT(`snow`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`snow_flag`), ',', 1)) AS `snow_flag`,
  IF(COUNT(`wdir`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`wdir_flag`), ',', 1)) AS `wdir_flag`,
  IF(COUNT(`wspd`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`wspd_flag`), ',', 1)) AS `wspd_flag`,
  IF(COUNT(`wpgt`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`wpgt_flag`), ',', 1)) AS `wpgt_flag`,
  IF(COUNT(`pres`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`pres_flag`), ',', 1)) AS `pres_flag`,
  IF(COUNT(`tsun`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`tsun_flag`), ',', 1)) AS `tsun_flag`
FROM (
  (
    SELECT
      `date`,
      `tavg`,
      `tmin`,
      `tmax`,
      `prcp`,
      `snow`,
      NULL AS `wdir`,
      `wspd`,
      `wpgt`,
      `pres`,
      `tsun`,
      IF(`tavg`, 'A', NULL) AS `tavg_flag`,
      IF(`tmin`, 'A', NULL) AS `tmin_flag`,
      IF(`tmax`, 'A', NULL) AS `tmax_flag`,
      IF(`prcp`, 'A', NULL) AS `prcp_flag`,
      IF(`snow`, 'A', NULL) AS `snow_flag`,
      NULL AS `wdir_flag`,
      IF(`wspd`, 'A', NULL) AS `wspd_flag`,
      IF(`wpgt`, 'A', NULL) AS `wpgt_flag`,
      IF(`pres`, 'A', NULL) AS `pres_flag`,
      IF(`tsun`, 'A', NULL) AS `tsun_flag`
    FROM
      `daily_national`
    WHERE
      `station` = :station
  ) UNION ALL
  (
    SELECT
      `date`,
      `tavg`,
      `tmin`,
      `tmax`,
      `prcp`,
      `snow`,
      `wdir`,
      `wspd`,
      `wpgt`,
      NULL AS `pres`,
      `tsun`,
      IF(`tavg`, 'B', NULL) AS `tavg_flag`,
      IF(`tmin`, 'B', NULL) AS `tmin_flag`,
      IF(`tmax`, 'B', NULL) AS `tmax_flag`,
      IF(`prcp`, 'B', NULL) AS `prcp_flag`,
      IF(`snow`, 'B', NULL) AS `snow_flag`,
      IF(`wdir`, 'B', NULL) AS `wdir_flag`,
      IF(`wspd`, 'B', NULL) AS `wspd_flag`,
      IF(`wpgt`, 'B', NULL) AS `wpgt_flag`,
      NULL AS `pres_flag`,
      IF(`tsun`, 'B', NULL) AS `tsun_flag`
    FROM
      `daily_ghcn`
    WHERE
      `station` = :station
  ) UNION ALL
  (
    SELECT
      DATE(`time`) AS `date`,
      IF(COUNT(`temp`) < 24, NULL, ROUND(AVG(`temp`), 1)) AS `tavg`,
      IF(COUNT(`temp`) < 24, NULL, MIN(`temp`)) AS `tmin`,
      IF(COUNT(`temp`) < 24, NULL, MAX(`temp`)) AS `tmax`,
      IF(COUNT(`prcp`) < 24, NULL, ROUND(SUM(`prcp`), 1)) AS `prcp`,
      IF(COUNT(`snow`) < 24, NULL, MAX(`snow`)) AS `snow`,
      IF(
        COUNT(`wdir`) < 24,
        NULL,
        ROUND(
          DEGAVG(
            SUM(
              SIN(RADIANS(`wdir`))
            ),
            SUM(
              COS(RADIANS(`wdir`))
            )
          ),
          1
        )
      ) AS `wdir`,
      IF(COUNT(`wspd`) < 24, NULL, ROUND(AVG(`wspd`), 1)) AS `wspd`,
      IF(COUNT(`wpgt`) < 24, NULL, MAX(`wpgt`)) AS `wpgt`,
      IF(COUNT(`pres`) < 24, NULL, ROUND(AVG(`pres`), 1)) AS `pres`,
      NULL AS `tsun`,
      GROUP_CONCAT(DISTINCT `temp_flag` SEPARATOR '') AS `tavg_flag`,
      GROUP_CONCAT(DISTINCT `temp_flag` SEPARATOR '') AS `tmin_flag`,
      GROUP_CONCAT(DISTINCT `temp_flag` SEPARATOR '') AS `tmax_flag`,
      GROUP_CONCAT(DISTINCT `prcp_flag` SEPARATOR '') AS `prcp_flag`,
      GROUP_CONCAT(DISTINCT `snow_flag` SEPARATOR '') AS `snow_flag`,
      GROUP_CONCAT(DISTINCT `wdir_flag` SEPARATOR '') AS `wdir_flag`,
      GROUP_CONCAT(DISTINCT `wspd_flag` SEPARATOR '') AS `wspd_flag`,
      GROUP_CONCAT(DISTINCT `wpgt_flag` SEPARATOR '') AS `wpgt_flag`,
      GROUP_CONCAT(DISTINCT `pres_flag` SEPARATOR '') AS `pres_flag`,
      GROUP_CONCAT(DISTINCT `tsun_flag` SEPARATOR '') AS `tsun_flag`
    FROM
      (
        SELECT
          CONVERT_TZ(MIN(`time`), 'UTC', :timezone) AS `time`,
          SUBSTRING_INDEX(GROUP_CONCAT(`temp` ORDER BY `flag` ASC), ',', 1) AS `temp`,
          SUBSTRING_INDEX(GROUP_CONCAT(`dwpt` ORDER BY `flag` ASC), ',', 1) AS `dwpt`,
          SUBSTRING_INDEX(GROUP_CONCAT(`rhum` ORDER BY `flag` ASC), ',', 1) AS `rhum`,
          SUBSTRING_INDEX(GROUP_CONCAT(`prcp` ORDER BY `flag` ASC), ',', 1) AS `prcp`,
          SUBSTRING_INDEX(GROUP_CONCAT(`snow` ORDER BY `flag` ASC), ',', 1) AS `snow`,
          SUBSTRING_INDEX(GROUP_CONCAT(`wdir` ORDER BY `flag` ASC), ',', 1) AS `wdir`,
          SUBSTRING_INDEX(GROUP_CONCAT(`wspd` ORDER BY `flag` ASC), ',', 1) AS `wspd`,
          SUBSTRING_INDEX(GROUP_CONCAT(`wpgt` ORDER BY `flag` ASC), ',', 1) AS `wpgt`,
          SUBSTRING_INDEX(GROUP_CONCAT(`pres` ORDER BY `flag` ASC), ',', 1) AS `pres`,
          SUBSTRING_INDEX(GROUP_CONCAT(`tsun` ORDER BY `flag` ASC), ',', 1) AS `tsun`,
          SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`temp`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `temp_flag`,
          SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`dwpt`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `dwpt_flag`,
          SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`rhum`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `rhum_flag`,
          SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`prcp`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `prcp_flag`,
          SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`snow`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `snow_flag`,
          SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`wdir`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `wdir_flag`,
          SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`wspd`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `wspd_flag`,
          SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`wpgt`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `wpgt_flag`,
          SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`pres`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `pres_flag`,
          SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`tsun`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `tsun_flag`
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
              'C' AS `flag`
            FROM
              `hourly_national`
            WHERE
              `station` = :station
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
              'D' AS `flag`
            FROM
              `hourly_isd`
            WHERE
              `station` = :station
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
              'E' AS `flag`
            FROM
              `hourly_synop`
            WHERE
              `station` = :station
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
              'F' AS `flag`
            FROM
              `hourly_metar`
            WHERE
              `station` = :station
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
              'G' AS `flag`
            FROM
              `hourly_model`
            WHERE
              `station` = :station
          )
        ) AS `hourly_derived`
        WHERE
          `time` <= DATE_ADD(NOW(), INTERVAL 10 DAY)
        GROUP BY
          DATE_FORMAT(`time`, '%Y %m %d %H')
        ORDER BY
          `time`
      ) AS `hourly_derived`
    GROUP BY
      `date`
  )
) AS `daily_derived`
WHERE
  (
    `tavg` IS NOT NULL OR
    `tmin` IS NOT NULL OR
    `tmax` IS NOT NULL OR
    `prcp` IS NOT NULL
  ) AND
  `date` <= DATE_ADD(CURRENT_DATE(), INTERVAL 10 DAY)
GROUP BY
  `date`
ORDER BY
  `date`
