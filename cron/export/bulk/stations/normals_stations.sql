    SELECT
        `id`,
        `latitude`,
        `longitude`,
        `altitude`
    FROM
        `stations`
    WHERE
        `id` IN (
            SELECT DISTINCT `station`
            FROM `inventory`
        ) OR
        `id` IN (
            SELECT DISTINCT `station`
            FROM `normals_global`
        )