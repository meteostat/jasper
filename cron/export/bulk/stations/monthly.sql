SET STATEMENT
  max_statement_time = 300
FOR
SELECT
  `year`,
  `month`,
  SUBSTRING_INDEX(GROUP_CONCAT(`tavg` ORDER BY `tavg_flag` ASC), ",", 1) AS `tavg`,
  SUBSTRING_INDEX(GROUP_CONCAT(`tmin` ORDER BY `tmin_flag` ASC), ",", 1) AS `tmin`,
  SUBSTRING_INDEX(GROUP_CONCAT(`tmax` ORDER BY `tmax_flag` ASC), ",", 1) AS `tmax`,
  SUBSTRING_INDEX(GROUP_CONCAT(`prcp` ORDER BY `prcp_flag` ASC), ",", 1) AS `prcp`,
  SUBSTRING_INDEX(GROUP_CONCAT(`wspd` ORDER BY `wspd_flag` ASC), ",", 1) AS `wspd`,
  SUBSTRING_INDEX(GROUP_CONCAT(`pres` ORDER BY `pres_flag` ASC), ",", 1) AS `pres`,
  SUBSTRING_INDEX(GROUP_CONCAT(`tsun` ORDER BY `tsun_flag` ASC), ",", 1) AS `tsun`,
  IF(COUNT(`tavg`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`tavg_flag`), ',', 1)) AS `tavg_flag`,
  IF(COUNT(`tmin`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`tmin_flag`), ',', 1)) AS `tmin_flag`,
  IF(COUNT(`tmax`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`tmax_flag`), ',', 1)) AS `tmax_flag`,
  IF(COUNT(`prcp`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`prcp_flag`), ',', 1)) AS `prcp_flag`,
  IF(COUNT(`wspd`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`wspd_flag`), ',', 1)) AS `wspd_flag`,
  IF(COUNT(`pres`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`pres_flag`), ',', 1)) AS `pres_flag`,
  IF(COUNT(`tsun`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`tsun_flag`), ',', 1)) AS `tsun_flag`
FROM (
  (
    SELECT
      `year`,
      `month`,
      `tavg`,
      `tmin`,
      `tmax`,
      `prcp`,
      NULL AS `wspd`,
      `pres`,
      `tsun`,
      IF(`tavg`, 'B', NULL) AS `tavg_flag`,
      IF(`tmin`, 'B', NULL) AS `tmin_flag`,
      IF(`tmax`, 'B', NULL) AS `tmax_flag`,
      IF(`prcp`, 'B', NULL) AS `prcp_flag`,
      NULL AS `wspd_flag`,
      IF(`pres`, 'B', NULL) AS `pres_flag`,
      IF(`tsun`, 'B', NULL) AS `tsun_flag`
    FROM
      `monthly_global`
    WHERE
      `station` = :station
  ) UNION ALL
  (
    SELECT
      YEAR(`date`) AS `year`,
      MONTH(`date`) AS `month`,
      IF(count(`tavg`) < DAY(LAST_DAY(`date`)), NULL, ROUND(AVG(`tavg`), 1)) AS `tavg`,
      IF(count(`tmin`) < DAY(LAST_DAY(`date`)), NULL, ROUND(AVG(`tmin`), 1)) AS `tmin`,
      IF(count(`tmax`) < DAY(LAST_DAY(`date`)), NULL, ROUND(AVG(`tmax`), 1)) AS `tmax`,
      IF(count(`prcp`) < DAY(LAST_DAY(`date`)), NULL, ROUND(SUM(`prcp`), 1)) AS `prcp`,
      IF(count(`wspd`) < DAY(LAST_DAY(`date`)), NULL, ROUND(AVG(`wspd`), 1)) AS `wspd`,
      IF(count(`pres`) < DAY(LAST_DAY(`date`)), NULL, ROUND(AVG(`pres`), 1)) AS `pres`,
      IF(count(`tsun`) < DAY(LAST_DAY(`date`)), NULL, ROUND(SUM(`tsun`), 1)) AS `tsun`,
      GROUP_CONCAT(DISTINCT `tavg_flag` SEPARATOR '') AS `tavg_flag`,
      GROUP_CONCAT(DISTINCT `tavg_flag` SEPARATOR '') AS `tmin_flag`,
      GROUP_CONCAT(DISTINCT `tavg_flag` SEPARATOR '') AS `tmax_flag`,
      GROUP_CONCAT(DISTINCT `prcp_flag` SEPARATOR '') AS `prcp_flag`,
      GROUP_CONCAT(DISTINCT `wspd_flag` SEPARATOR '') AS `wspd_flag`,
      GROUP_CONCAT(DISTINCT `pres_flag` SEPARATOR '') AS `pres_flag`,
      GROUP_CONCAT(DISTINCT `tsun_flag` SEPARATOR '') AS `tsun_flag`
    FROM
      (
        SELECT
          `date`,
          SUBSTRING_INDEX(GROUP_CONCAT(`tavg` ORDER BY `tavg_flag` ASC), ",", 1) AS `tavg`,
          SUBSTRING_INDEX(GROUP_CONCAT(`tmin` ORDER BY `tmin_flag` ASC), ",", 1) AS `tmin`,
          SUBSTRING_INDEX(GROUP_CONCAT(`tmax` ORDER BY `tmax_flag` ASC), ",", 1) AS `tmax`,
          SUBSTRING_INDEX(GROUP_CONCAT(`prcp` ORDER BY `prcp_flag` ASC), ",", 1) AS `prcp`,
          SUBSTRING_INDEX(GROUP_CONCAT(`wspd` ORDER BY `wspd_flag` ASC), ",", 1) AS `wspd`,
          SUBSTRING_INDEX(GROUP_CONCAT(`pres` ORDER BY `pres_flag` ASC), ",", 1) AS `pres`,
          SUBSTRING_INDEX(GROUP_CONCAT(`tsun` ORDER BY `tsun_flag` ASC), ",", 1) AS `tsun`,
          IF(COUNT(`tavg`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`tavg_flag`), ',', 1)) AS `tavg_flag`,
          IF(COUNT(`tmin`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`tmin_flag`), ',', 1)) AS `tmin_flag`,
          IF(COUNT(`tmax`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`tmax_flag`), ',', 1)) AS `tmax_flag`,
          IF(COUNT(`prcp`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`prcp_flag`), ',', 1)) AS `prcp_flag`,
          IF(COUNT(`wspd`) = 0, NULL, SUBSTRING_INDEX(GROUP_CONCAT(`wspd_flag`), ',', 1)) AS `wspd_flag`,
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
              `wspd`,
              `pres`,
              `tsun`,
              IF(`tavg`, 'C', NULL) AS `tavg_flag`,
              IF(`tmin`, 'C', NULL) AS `tmin_flag`,
              IF(`tmax`, 'C', NULL) AS `tmax_flag`,
              IF(`prcp`, 'C', NULL) AS `prcp_flag`,
              IF(`wspd`, 'C', NULL) AS `wspd_flag`,
              IF(`pres`, 'C', NULL) AS `pres_flag`,
              IF(`tsun`, 'C', NULL) AS `tsun_flag`
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
              `wspd`,
              NULL AS `pres`,
              `tsun`,
              IF(`tavg`, 'D', NULL) AS `tavg_flag`,
              IF(`tmin`, 'D', NULL) AS `tmin_flag`,
              IF(`tmax`, 'D', NULL) AS `tmax_flag`,
              IF(`prcp`, 'D', NULL) AS `prcp_flag`,
              IF(`wspd`, 'D', NULL) AS `wspd_flag`,
              NULL AS `pres_flag`,
              IF(`tsun`, 'D', NULL) AS `tsun_flag`
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
              IF(COUNT(`wspd`) < 24, NULL, ROUND(AVG(`wspd`), 1)) AS `wspd`,
              IF(COUNT(`pres`) < 24, NULL, ROUND(AVG(`pres`), 1)) AS `pres`,
              NULL AS `tsun`,
              GROUP_CONCAT(DISTINCT `temp_flag` SEPARATOR '') AS `tavg_flag`,
              GROUP_CONCAT(DISTINCT `temp_flag` SEPARATOR '') AS `tmin_flag`,
              GROUP_CONCAT(DISTINCT `temp_flag` SEPARATOR '') AS `tmax_flag`,
              GROUP_CONCAT(DISTINCT `prcp_flag` SEPARATOR '') AS `prcp_flag`,
              GROUP_CONCAT(DISTINCT `wspd_flag` SEPARATOR '') AS `wspd_flag`,
              GROUP_CONCAT(DISTINCT `pres_flag` SEPARATOR '') AS `pres_flag`,
              GROUP_CONCAT(DISTINCT `tsun_flag` SEPARATOR '') AS `tsun_flag`
            FROM (
              SELECT
                CONVERT_TZ(MIN(`time`), 'UTC', :timezone) AS `time`,
                SUBSTRING_INDEX(GROUP_CONCAT(`temp` ORDER BY `flag` ASC), ',', 1) AS `temp`,
                SUBSTRING_INDEX(GROUP_CONCAT(`prcp` ORDER BY `flag` ASC), ',', 1) AS `prcp`,
                SUBSTRING_INDEX(GROUP_CONCAT(`wspd` ORDER BY `flag` ASC), ',', 1) AS `wspd`,
                SUBSTRING_INDEX(GROUP_CONCAT(`pres` ORDER BY `flag` ASC), ',', 1) AS `pres`,
                SUBSTRING_INDEX(GROUP_CONCAT(`tsun` ORDER BY `flag` ASC), ',', 1) AS `tsun`,
                SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`temp`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `temp_flag`,
                SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`prcp`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `prcp_flag`,
                SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`wspd`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `wspd_flag`,
                SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`pres`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `pres_flag`,
                SUBSTR(SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(`tsun`, ':', `flag`) ORDER BY `flag` ASC), ',', 1), -1, 1) AS `tsun_flag`
              FROM (
                (
                  SELECT
                    `time`,
                    `temp`,
                    `prcp`,
                    `wspd`,
                    `pres`,
                    `tsun`,
                    'E' AS `flag`
                  FROM
                    `hourly_national`
                  WHERE
                    `station` = :station
                ) UNION ALL
                (
                  SELECT
                    `time`,
                    `temp`,
                    `prcp`,
                    `wspd`,
                    `pres`,
                    NULL AS `tsun`,
                    'F' AS `flag`
                  FROM
                    `hourly_isd`
                  WHERE
                    `station` = :station
                ) UNION ALL
                (
                  SELECT
                    `time`,
                    `temp`,
                    `prcp`,
                    `wspd`,
                    `pres`,
                    `tsun`,
                    'G' AS `flag`
                  FROM
                    `hourly_synop`
                  WHERE
                    `station` = :station
                ) UNION ALL
                (
                  SELECT
                    `time`,
                    `temp`,
                    NULL AS `prcp`,
                    `wspd`,
                    `pres`,
                    NULL AS `tsun`,
                    'H' AS `flag`
                  FROM
                    `hourly_metar`
                  WHERE
                    `station` = :station
                ) UNION ALL
                (
                  SELECT
                    `time`,
                    `temp`,
                    `prcp`,
                    `wspd`,
                    `pres`,
                    NULL AS `tsun`,
                    'I' AS `flag`
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
      ) AS `daily_derived`
      GROUP BY
        `year`,
        `month`
  )
) AS `monthly_derived`
WHERE
  `tavg` IS NOT NULL
  OR `tmin` IS NOT NULL
  OR `tmax` IS NOT NULL
  OR `prcp` IS NOT NULL
GROUP BY
  `year`, `month`
ORDER BY
  `year`, `month`;
