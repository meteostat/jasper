SELECT `id`,
       `latitude`,
       `longitude`,
       `altitude`
FROM   `stations`
WHERE  `id` in (SELECT DISTINCT `station`
                FROM   `inventory`) or `id` in (SELECT DISTINCT `station`
                                FROM   `normals_global`);
