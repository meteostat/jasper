SET STATEMENT
  max_statement_time = 60
FOR
SELECT
  `date`,
  SUBSTRING_INDEX(GROUP_CONCAT(`tavg` ORDER BY `priority` ASC), ",", 1) AS `tavg`,
  SUBSTRING_INDEX(GROUP_CONCAT(`tmin` ORDER BY `priority` ASC), ",", 1) AS `tmin`,
  SUBSTRING_INDEX(GROUP_CONCAT(`tmax` ORDER BY `priority` ASC), ",", 1) AS `tmax`,
  SUBSTRING_INDEX(GROUP_CONCAT(`prcp` ORDER BY `priority` ASC), ",", 1) AS `prcp`,
  SUBSTRING_INDEX(GROUP_CONCAT(`snow` ORDER BY `priority` ASC), ",", 1) AS `snow`,
  SUBSTRING_INDEX(GROUP_CONCAT(`wdir` ORDER BY `priority` ASC), ",", 1) AS `wdir`,
  SUBSTRING_INDEX(GROUP_CONCAT(`wspd` ORDER BY `priority` ASC), ",", 1) AS `wspd`,
  SUBSTRING_INDEX(GROUP_CONCAT(`wpgt` ORDER BY `priority` ASC), ",", 1) AS `wpgt`,
  SUBSTRING_INDEX(GROUP_CONCAT(`pres` ORDER BY `priority` ASC), ",", 1) AS `pres`,
  SUBSTRING_INDEX(GROUP_CONCAT(`tsun` ORDER BY `priority` ASC), ",", 1) AS `tsun`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`tavg`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `tavg_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`tmin`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `tmin_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`tmax`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `tmax_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`prcp`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `prcp_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`snow`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `snow_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`wdir`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `wdir_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`wspd`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `wspd_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`wpgt`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `wpgt_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`pres`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `pres_flag`,
  SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`tsun`, ':', `priority`) ORDER BY `priority` ASC), ',', 1), -1, 1) AS `tsun_flag`
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
      'A' AS `priority`
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
      'B' AS `priority`
    FROM
      `daily_ghcn`
    WHERE
      `station` = :station
  ) UNION ALL
  (
    SELECT
      DATE(CONVERT_TZ(`hourly_national`.`time`, "UTC", :timezone)) AS `date`,
      IF(COUNT(`hourly_national`.`temp`) < 24, NULL, ROUND(AVG(`hourly_national`.`temp`), 1)) AS `tavg`,
      IF(COUNT(`hourly_national`.`temp`) < 24, NULL, MIN(`hourly_national`.`temp`)) AS `tmin`,
      IF(COUNT(`hourly_national`.`temp`) < 24, NULL, MAX(`hourly_national`.`temp`)) AS `tmax`,
      IF(COUNT(`hourly_national`.`prcp`) < 24, NULL, SUM(`hourly_national`.`prcp`)) AS `prcp`,
      NULL AS `snow`,
      IF(
        COUNT(`hourly_national`.`wdir`) < 24,
        NULL,
        ROUND(
          DEGAVG(
            SUM(
              SIN(RADIANS(`hourly_national`.`wdir`))
            ),
            SUM(
              COS(RADIANS(`hourly_national`.`wdir`))
            )
          ),
          1
        )
      ) AS `wdir`,
      IF(COUNT(`hourly_national`.`wspd`) < 24, NULL, ROUND(AVG(`hourly_national`.`wspd`), 1)) AS `wspd`,
      NULL AS `wpgt`,
      IF(COUNT(`hourly_national`.`pres`) < 24, NULL, ROUND(AVG(`hourly_national`.`pres`), 1)) AS `pres`,
      NULL AS `tsun`,
      'C' AS `priority`
    FROM
      `hourly_national`
    WHERE
      `hourly_national`.`station` = :station
    GROUP BY
      `station`,
      `date`
  ) UNION ALL
  (
    SELECT
      DATE(CONVERT_TZ(`hourly_isd`.`time`, "UTC", :timezone)) AS `date`,
      IF(COUNT(`hourly_isd`.`temp`) < 24, NULL, ROUND(AVG(`hourly_isd`.`temp`),1)) AS `tavg`,
      IF(COUNT(`hourly_isd`.`temp`) < 24, NULL, MIN(`hourly_isd`.`temp`)) AS `tmin`,
      IF(COUNT(`hourly_isd`.`temp`) < 24, NULL, MAX(`hourly_isd`.`temp`)) AS `tmax`,
      IF(COUNT(`hourly_isd`.`prcp`) < 24, NULL, SUM(`hourly_isd`.`prcp`)) AS `prcp`,
      NULL AS `snow`,
      IF(
        COUNT(`hourly_isd`.`wdir`) < 24,
        NULL,
        ROUND(
          DEGAVG(
            SUM(
              SIN(RADIANS(`hourly_isd`.`wdir`))
            ),
            SUM(
              COS(RADIANS(`hourly_isd`.`wdir`))
            )
          ),
          1
        )
      ) AS `wdir`,
      IF(COUNT(`hourly_isd`.`wspd`) < 24, NULL, ROUND(AVG(`hourly_isd`.`wspd`),1)) AS `wspd`,
      NULL AS `wpgt`,
      IF(COUNT(`hourly_isd`.`pres`) < 24, NULL, ROUND(AVG(`hourly_isd`.`pres`),1)) AS `pres`,
      NULL AS `tsun`,
      'D' AS `priority`
    FROM
      `hourly_isd`
    WHERE
      `hourly_isd`.`station` = :station
    GROUP BY
      `station`,
      `date`
  ) UNION ALL
  (
    SELECT
      DATE(CONVERT_TZ(`hourly_synop`.`time`, "UTC", :timezone)) AS `date`,
      IF(COUNT(`hourly_synop`.`temp`) < 24, NULL, ROUND(AVG(`hourly_synop`.`temp`),1)) AS `tavg`,
      IF(COUNT(`hourly_synop`.`temp`) < 24, NULL, MIN(`hourly_synop`.`temp`)) AS `tmin`,
      IF(COUNT(`hourly_synop`.`temp`) < 24, NULL, MAX(`hourly_synop`.`temp`)) AS `tmax`,
      IF(COUNT(`hourly_synop`.`prcp`) < 24, NULL, SUM(`hourly_synop`.`prcp`)) AS `prcp`,
      IF(COUNT(`hourly_synop`.`snow`) < 24, NULL, MAX(`hourly_synop`.`snow`)) AS `snow`,
      IF(
        COUNT(`hourly_synop`.`wdir`) < 24,
        NULL,
        ROUND(
          DEGAVG(
            SUM(
              SIN(RADIANS(`hourly_synop`.`wdir`))
            ),
            SUM(
              COS(RADIANS(`hourly_synop`.`wdir`))
            )
          ),
          1
        )
      ) AS `wdir`,
      IF(COUNT(`hourly_synop`.`wspd`) < 24, NULL, ROUND(AVG(`hourly_synop`.`wspd`),1)) AS `wspd`,
      IF(COUNT(`hourly_synop`.`wpgt`) < 24, NULL, MAX(`wpgt`)) AS `wpgt`,
      IF(COUNT(`hourly_synop`.`pres`) < 24, NULL, ROUND(AVG(`hourly_synop`.`pres`),1)) AS `pres`,
      NULL AS `tsun`,
      'E' AS `priority`
    FROM
      `hourly_synop`
    WHERE
      `hourly_synop`.`station` = :station
    GROUP BY
      `station`,
      `date`
  ) UNION ALL
  (
    SELECT
      DATE(CONVERT_TZ(`hourly_metar`.`time`, "UTC", :timezone)) AS `date`,
      IF(COUNT(`hourly_metar`.`temp`) < 24, NULL, ROUND(AVG(`hourly_metar`.`temp`),1)) AS `tavg`,
      IF(COUNT(`hourly_metar`.`temp`) < 24, NULL, MIN(`hourly_metar`.`temp`)) AS `tmin`,
      IF(COUNT(`hourly_metar`.`temp`) < 24, NULL, MAX(`hourly_metar`.`temp`)) AS `tmax`,
      NULL AS `prcp`,
      NULL AS `snow`,
      IF(
        COUNT(`hourly_metar`.`wdir`) < 24,
        NULL,
        ROUND(
          DEGAVG(
            SUM(
              SIN(RADIANS(`hourly_metar`.`wdir`))
            ),
            SUM(
              COS(RADIANS(`hourly_metar`.`wdir`))
            )
          ),
          1
        )
      ) AS `wdir`,
      IF(COUNT(`hourly_metar`.`wspd`) < 24, NULL, ROUND(AVG(`hourly_metar`.`wspd`),1)) AS `wspd`,
      NULL AS `wpgt`,
      IF(COUNT(`hourly_metar`.`pres`) < 24, NULL, ROUND(AVG(`hourly_metar`.`pres`),1)) AS `pres`,
      NULL AS `tsun`,
      'F' AS `priority`
    FROM
      `hourly_metar`
    WHERE
      `hourly_metar`.`station` = :station
    GROUP BY
      `station`,
      `date`
  ) UNION ALL
  (
    SELECT
      DATE(CONVERT_TZ(`hourly_model`.`time`, "UTC", :timezone)) AS `date`,
      IF(COUNT(`hourly_model`.`temp`) < 24, NULL, ROUND(AVG(`hourly_model`.`temp`),1)) AS `tavg`,
      IF(COUNT(`hourly_model`.`temp`) < 24, NULL, MIN(`hourly_model`.`temp`)) AS `tmin`,
      IF(COUNT(`hourly_model`.`temp`) < 24, NULL, MAX(`hourly_model`.`temp`)) AS `tmax`,
      IF(COUNT(`hourly_model`.`prcp`) < 24, NULL, SUM(`hourly_model`.`prcp`)) AS `prcp`,
      NULL AS `snow`,
      IF(
        COUNT(`hourly_model`.`wdir`) < 24,
        NULL,
        ROUND(
          DEGAVG(
            SUM(
              SIN(RADIANS(`hourly_model`.`wdir`))
            ),
            SUM(
              COS(RADIANS(`hourly_model`.`wdir`))
            )
          ),
          1
        )
      ) AS `wdir`,
      IF(COUNT(`hourly_model`.`wspd`) < 24, NULL, ROUND(AVG(`hourly_model`.`wspd`), 1)) AS `wspd`,
      IF(COUNT(`hourly_model`.`wpgt`) < 24, NULL, MAX(`hourly_model`.`wpgt`)) AS `wpgt`,
      IF(COUNT(`hourly_model`.`pres`) < 24, NULL, ROUND(AVG(`hourly_model`.`pres`), 1)) AS `pres`,
      NULL AS `tsun`,
      'G' AS `priority`
    FROM
      `hourly_model`
    WHERE
      `hourly_model`.`station` = :station
    GROUP BY
      `station`,
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