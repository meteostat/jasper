SELECT `stations`.`id` AS `id`
FROM   `stations`
WHERE  `stations`.`id` IN (SELECT DISTINCT `station`
                           FROM   `inventory`
                           WHERE  `mode` IN ( 'H', 'P' )
                                  AND `end` >= Date(Now()))
       AND `stations`.`id` NOT IN (SELECT DISTINCT `stations`.`id` AS `id`
                                   FROM   `stations`
                                   WHERE  `stations`.`id` IN
                                          (SELECT DISTINCT `station`
                                           FROM   `inventory`
                                           WHERE  `mode` = 'H'
                                                  AND `end` >= Date(
                                                      Now(
                                                      ) -
                                          INTERVAL 2 day
                                                      ))) 