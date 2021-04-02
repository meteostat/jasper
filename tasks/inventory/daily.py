"""
Update daily inventory

The code is licensed under the MIT license.
"""

from routines import Routine

task = Routine('task.inventory.daily')

task.query('''
    INSERT INTO
        `inventory`(`station`, `mode`, `start`)
    SELECT
        `station`,
        'D' AS `mode`,
        MIN(`mindate`) AS `start` FROM (
            (SELECT
                `station`,
                MIN(`date`) as `mindate`
            FROM `daily_national`
            GROUP BY `station`)
        UNION ALL
            (SELECT
                `station`,
                MIN(`date`) as `mindate`
            FROM `daily_ghcn`
            GROUP BY `station`)
        ) AS `daily_inventory`
    GROUP BY `station`
    ON DUPLICATE KEY UPDATE
        `start` = VALUES(`start`)
''')

task.query('''
    INSERT INTO
        `inventory`(`station`, `mode`, `end`)
    SELECT
        `station`,
        'D' AS `mode`,
        MAX(`maxdate`) AS `end` FROM (
            (SELECT
                `station`,
                MAX(`date`) as `maxdate`
            FROM `daily_national`
            GROUP BY `station`)
        UNION ALL
            (SELECT
                `station`,
                MAX(`date`) as `maxdate`
            FROM `daily_ghcn`
            GROUP BY `station`)
        ) AS `daily_inventory`
    GROUP BY `station`
    ON DUPLICATE KEY UPDATE
        `end` = VALUES(`end`)
''')

# Legacy
task.query("INSERT INTO `stations_inventory`(`station`, `daily_start`) SELECT `station`,MIN(`mindate`) AS `daily_start` FROM ((SELECT `station`,MIN(`date`) as `mindate` FROM `daily_national` GROUP BY `station`) UNION ALL (SELECT `station`,MIN(`date`) as `mindate` FROM `daily_ghcn` GROUP BY `station`)) AS `daily_inventory` GROUP BY `station` ON DUPLICATE KEY UPDATE `daily_start` = VALUES(`daily_start`)")

task.query("INSERT INTO `stations_inventory`(`station`, `daily_end`) SELECT `station`,MAX(`maxdate`) AS `daily_end` FROM ((SELECT `station`,MAX(`date`) as `maxdate` FROM `daily_national` GROUP BY `station`) UNION ALL (SELECT `station`,MAX(`date`) as `maxdate` FROM `daily_ghcn` GROUP BY `station`)) AS `daily_inventory` GROUP BY `station` ON DUPLICATE KEY UPDATE `daily_end` = VALUES(`daily_end`)")
