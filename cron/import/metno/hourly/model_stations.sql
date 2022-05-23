SELECT
    `stations`.`id` AS `id`,
    `stations`.`latitude` AS `latitude`,
    `stations`.`longitude` AS `longitude`,
    `stations`.`altitude` AS `altitude`
FROM `stations`
WHERE
    `stations`.`latitude` IS NOT NULL AND
    `stations`.`longitude` IS NOT NULL AND
    `stations`.`altitude` IS NOT NULL AND
    `stations`.`id` IN (
        SELECT DISTINCT `station`
        FROM `inventory`
        WHERE
            `mode` IN ('H', 'D') AND
            `end` > DATE_SUB(NOW(), INTERVAL 1095 DAY)
    )