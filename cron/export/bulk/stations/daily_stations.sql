SELECT `stations`.`id` as `id`,
       `stations`.`tz` as `tz`
FROM   `stations`
WHERE  `stations`.`id` in (SELECT DISTINCT `station`
                           FROM   `inventory`
                           WHERE  `mode` in ('D', 'H', 'P'));
