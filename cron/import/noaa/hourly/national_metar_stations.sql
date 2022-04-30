SELECT
    `stations`.`id` AS `id`,
    `stations`.`icao` AS `icao`
FROM `stations`
WHERE
  `country` LIKE 'US' AND
  `icao` IS NOT NULL