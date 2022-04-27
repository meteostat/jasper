SELECT
  `stations`.`id` AS `id`,
  `stations`.`name` AS `name`,
  `stations`.`name_alt` AS `name_alt`,
  `stations`.`country` AS `country`,
  `stations`.`region` AS `region`,
  `stations`.`national_id` AS `national_id`,
  CAST(`stations`.`wmo` AS CHAR(5)) AS `wmo`,
  `stations`.`icao` AS `icao`,
  `stations`.`latitude` AS `latitude`,
  `stations`.`longitude` AS `longitude`,
  `stations`.`altitude` AS `altitude`,
  `stations`.`tz` as `timezone`,
  `stations`.`history` as `history`,
  MIN(`inventory_model`.`start`) AS "model_start",
  MAX(`inventory_model`.`end`) AS "model_end",
  MIN(`inventory_hourly`.`start`) AS "hourly_start",
  MAX(`inventory_hourly`.`end`) AS "hourly_end",
  MIN(`inventory_daily`.`start`) AS "daily_start",
  MAX(`inventory_daily`.`end`) AS "daily_end",
  YEAR(MIN(`inventory_monthly`.`start`)) AS "monthly_start",
  YEAR(MAX(`inventory_monthly`.`end`)) AS "monthly_end",
  YEAR(MIN(`inventory_normals`.`start`)) AS "normals_start",
  YEAR(MAX(`inventory_normals`.`end`)) AS "normals_end"
FROM
  `stations`
LEFT JOIN (
  SELECT
    `station`,
    `start`,
    `end`
  FROM
    `inventory`
  WHERE
    `mode` = "P"
)
AS
  `inventory_model`
ON
  `stations`.`id` = `inventory_model`.`station`
LEFT JOIN (
  SELECT
    `station`,
    `start`,
    `end`
  FROM
    `inventory`
  WHERE
    `mode` = "H"
)
AS
  `inventory_hourly`
ON
  `stations`.`id` = `inventory_hourly`.`station`
LEFT JOIN (
  SELECT
    `station`,
    `start`,
    `end`
  FROM `inventory`
  WHERE
    `mode` = "D"
)
AS
  `inventory_daily`
ON
    `stations`.`id` = `inventory_daily`.`station`
LEFT JOIN (
    SELECT
        `station`,
        `start`,
        `end`
    FROM `inventory`
    WHERE
        `mode` = "M"
)
AS
    `inventory_monthly`
ON
    `stations`.`id` = `inventory_monthly`.`station`
LEFT JOIN (
    SELECT
        `station`,
        `start`,
        `end`
    FROM `inventory`
    WHERE
        `mode` = "N"
)
AS
    `inventory_normals`
ON
    `stations`.`id` = `inventory_normals`.`station`
GROUP BY
    `stations`.`id`