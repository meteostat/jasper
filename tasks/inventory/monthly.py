"""
Update monthly inventory

The code is licensed under the MIT license.
"""

from routines import Routine

task = Routine('task.inventory.monthly')

task.query('''
    INSERT INTO
        `inventory`(`station`, `mode`, `start`)
    SELECT
        `station`,
        'M' AS `mode`,
        MIN(`mindate`) AS `start` FROM (
            (SELECT
                `station`,
                CONCAT(MIN(`year`), '-01-01') as `mindate`
            FROM `monthly_global`
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
        'M' AS `mode`,
        MAX(`maxdate`) AS `end` FROM (
            (SELECT
                `station`,
                CONCAT(MAX(`year`), '-12-31') as `maxdate`
            FROM `monthly_global`
            GROUP BY `station`)
        ) AS `daily_inventory`
    GROUP BY `station`
    ON DUPLICATE KEY UPDATE
        `end` = VALUES(`end`)
''')
