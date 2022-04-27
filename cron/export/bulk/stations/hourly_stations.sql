SELECT
  `stations`.`id` AS `id`
FROM `stations`
WHERE
  `stations`.`id` IN (
    SELECT DISTINCT `station`
    FROM `inventory`
    WHERE
      `mode` IN ('H', 'P')
  )