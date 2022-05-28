SELECT `stations`.`id` as `id`
FROM   `stations`
WHERE  `stations`.`id` in (SELECT DISTINCT `station`
                           FROM   `inventory`
                           WHERE  `mode` = 'H' AND `end` >= DATE(NOW() - INTERVAL 2 DAY))