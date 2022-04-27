"""
Update hourly inventory

The code is licensed under the MIT license.
"""

from jasper import Jasper


# Create Jasper instance
jsp = Jasper("task.inventory.hourly")

jsp.query(
    """
    INSERT INTO
        `inventory`(`station`, `mode`, `start`)
    SELECT
        `station`,
        'H' AS `mode`,
        MIN(`mindate`) AS `start` FROM (
            (SELECT
                `station`,
                DATE(MIN(`time`)) as `mindate`
            FROM `hourly_synop`
            GROUP BY `station`)
        UNION ALL
            (SELECT
                `station`,
                DATE(MIN(`time`)) as `mindate`
            FROM `hourly_metar`
            GROUP BY `station`)
        UNION ALL
            (SELECT
                `station`,
                DATE(MIN(`time`)) as `mindate`
            FROM `hourly_national`
            GROUP BY `station`)
        UNION ALL
            (SELECT
                `station`,
                DATE(MIN(`time`)) as `mindate`
            FROM `hourly_isd`
            GROUP BY `station`)
        ) AS `hourly_inventory`
    GROUP BY `station`
    ON DUPLICATE KEY UPDATE
        `start` = VALUES(`start`)
"""
)

jsp.query(
    """
    INSERT INTO
        `inventory`(`station`, `mode`, `start`)
    SELECT
        `station`,
        'P' AS `mode`,
        MIN(`mindate`) AS `start` FROM (
            (SELECT
                `station`,
                DATE(MIN(`time`)) as `mindate`
            FROM `hourly_model`
            GROUP BY `station`)
        ) AS `model_inventory`
    GROUP BY `station`
    ON DUPLICATE KEY UPDATE
        `start` = VALUES(`start`)
"""
)

jsp.query(
    """
    INSERT INTO
        `inventory`(`station`, `mode`, `end`)
    SELECT
        `station`,
        'H' AS `mode`,
        MAX(`maxdate`) AS `end` FROM (
            (SELECT
                `station`,
                DATE(MAX(`time`)) as `maxdate`
            FROM `hourly_synop`
            GROUP BY `station`)
        UNION ALL
            (SELECT
                `station`,
                DATE(MAX(`time`)) as `maxdate`
            FROM `hourly_metar`
            GROUP BY `station`)
        UNION ALL
            (SELECT
                `station`,
                DATE(MAX(`time`)) as `maxdate`
            FROM `hourly_national`
            GROUP BY `station`)
        UNION ALL
            (SELECT
                `station`,
                DATE(MAX(`time`)) as `maxdate`
            FROM `hourly_isd`
            GROUP BY `station`)
        ) AS `hourly_inventory`
    GROUP BY `station`
    ON DUPLICATE KEY UPDATE
        `end` = VALUES(`end`)
"""
)

jsp.query(
    """
    INSERT INTO
        `inventory`(`station`, `mode`, `end`)
    SELECT
        `station`,
        'P' AS `mode`,
        MAX(`maxdate`) AS `end` FROM (
            (SELECT
                `station`,
                DATE(MAX(`time`)) as `maxdate`
            FROM `hourly_model`
            GROUP BY `station`)
        ) AS `model_inventory`
    GROUP BY `station`
    ON DUPLICATE KEY UPDATE
        `end` = VALUES(`end`)
"""
)

# Legacy
# pylint: disable=line-too-long
jsp.query(
    "INSERT INTO `stations_inventory`(`station`, `hourly_start`) SELECT `station`, MIN(`mindate`) AS `hourly_start` FROM ((SELECT `station`,DATE(MIN(`time`)) as `mindate` FROM `hourly_model` GROUP BY `station`) UNION ALL (SELECT `station`,DATE(MIN(`time`)) as `mindate` FROM `hourly_metar` GROUP BY `station`) UNION ALL (SELECT `station`,DATE(MIN(`time`)) as `mindate` FROM `hourly_synop` GROUP BY `station`) UNION ALL (SELECT `station`,DATE(MIN(`time`)) as `mindate` FROM `hourly_national` GROUP BY `station`) UNION ALL (SELECT `station`,DATE(MIN(`time`)) as `mindate` FROM `hourly_isd` GROUP BY `station`)) AS `hourly_inventory` GROUP BY `station` ON DUPLICATE KEY UPDATE `hourly_start` = VALUES(`hourly_start`)"
)
jsp.query(
    "INSERT INTO `stations_inventory`(`station`, `hourly_end`) SELECT `station`, MAX(`maxdate`) AS `hourly_end` FROM ((SELECT `station`,DATE(MAX(`time`)) as `maxdate` FROM `hourly_model` GROUP BY `station`) UNION ALL (SELECT `station`,DATE(MAX(`time`)) as `maxdate` FROM `hourly_metar` GROUP BY `station`) UNION ALL (SELECT `station`,DATE(MAX(`time`)) as `maxdate` FROM `hourly_synop` GROUP BY `station`) UNION ALL (SELECT `station`,DATE(MAX(`time`)) as `maxdate` FROM `hourly_national` GROUP BY `station`) UNION ALL (SELECT `station`,DATE(MAX(`time`)) as `maxdate` FROM `hourly_isd` GROUP BY `station`)) AS `hourly_inventory` GROUP BY `station` ON DUPLICATE KEY UPDATE `hourly_end` = VALUES(`hourly_end`)"
)

# Close Jasper instance
jsp.close()