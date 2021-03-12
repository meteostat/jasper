"""
Export hourly bulk data

The code is licensed under the MIT license.
"""

from routines import Routine

# Configuration
STATIONS_PER_CYCLE = 1

task = Routine('export.bulk.hourly')

stations = task.get_stations("""
    SELECT
        `stations`.`id` AS `id`
    FROM `stations`
    WHERE
        `stations`.`id` IN (
            SELECT DISTINCT `station`
            FROM `inventory`
            WHERE
                `mode` = 'H'
        )
""", STATIONS_PER_CYCLE)

# Export data for each weather station
for station in stations:

    df = task.read(f"""
		SET STATEMENT
			max_statement_time=90
		FOR
		SELECT
			DATE(MIN(`time`)) AS `date`,
			DATE_FORMAT(MIN(`time`), '%H') AS `hour`,
			SUBSTRING_INDEX(GROUP_CONCAT(`temp` ORDER BY `priority`), ',', 1) AS `temp`,
			SUBSTRING_INDEX(GROUP_CONCAT(`dwpt` ORDER BY `priority`), ',', 1) AS `dwpt`,
			SUBSTRING_INDEX(GROUP_CONCAT(`rhum` ORDER BY `priority`), ',', 1) AS `rhum`,
			SUBSTRING_INDEX(GROUP_CONCAT(`prcp` ORDER BY `priority`), ',', 1) AS `prcp`,
			SUBSTRING_INDEX(GROUP_CONCAT(`snow` ORDER BY `priority`), ',', 1) AS `snow`,
			SUBSTRING_INDEX(GROUP_CONCAT(`wdir` ORDER BY `priority`), ',', 1) AS `wdir`,
			SUBSTRING_INDEX(GROUP_CONCAT(`wspd` ORDER BY `priority`), ',', 1) AS `wspd`,
			SUBSTRING_INDEX(GROUP_CONCAT(`wpgt` ORDER BY `priority`), ',', 1) AS `wpgt`,
			SUBSTRING_INDEX(GROUP_CONCAT(`pres` ORDER BY `priority`), ',', 1) AS `pres`,
			SUBSTRING_INDEX(GROUP_CONCAT(`tsun` ORDER BY `priority`), ',', 1) AS `tsun`,
			SUBSTRING_INDEX(GROUP_CONCAT(`coco` ORDER BY `priority`), ',', 1) AS `coco`
		FROM (
			(SELECT
				`time`,
				`temp`,
				ROUND((243.04*(LN(`rhum`/100)+((17.625*`temp`)/(243.04+`temp`)))/(17.625-LN(`rhum`/100)-((17.625*`temp`)/(243.04+`temp`)))),1) AS `dwpt`,
				`rhum`,
				`prcp`,
				NULL AS `snow`,
				`wdir`,
				`wspd`,
				NULL AS `wpgt`,
				`pres`,
				`tsun`,
				NULL AS `coco`,
				'A' AS `priority`
			FROM `hourly_national`
			WHERE
				`station` = '{station[0]}'
			)
		UNION ALL
			(SELECT
				`time`,
				`temp`,
				ROUND((243.04*(LN(`rhum`/100)+((17.625*`temp`)/(243.04+`temp`)))/(17.625-LN(`rhum`/100)-((17.625*`temp`)/(243.04+`temp`)))),1) AS `dwpt`,
				`rhum`,
				`prcp`,
				NULL AS `snow`,
				`wdir`,
				`wspd`,
				NULL AS `wpgt`,
				`pres`,
				NULL AS `tsun`,
				NULL AS `coco`,
				'A' AS `priority`
			FROM `hourly_isd`
			WHERE
				`station` = '{station[0]}'
			)
		UNION ALL
			(SELECT
				`time`,
				`temp`,
				ROUND((243.04*(LN(`rhum`/100)+((17.625*`temp`)/(243.04+`temp`)))/(17.625-LN(`rhum`/100)-((17.625*`temp`)/(243.04+`temp`)))),1) AS `dwpt`,
				`rhum`,
				`prcp`,
				`snow`,
				`wdir`,
				`wspd`,
				`wpgt`,
				`pres`,
				`tsun`,
				`coco`,
				'B' AS `priority`
			FROM `hourly_synop`
			WHERE
				`station` = '{station[0]}'
			)
		UNION ALL
			(SELECT
				`time`,
				`temp`,
				ROUND((243.04*(LN(`rhum`/100)+((17.625*`temp`)/(243.04+`temp`)))/(17.625-LN(`rhum`/100)-((17.625*`temp`)/(243.04+`temp`)))),1) AS `dwpt`,
				`rhum`,
				NULL AS `prcp`,
				NULL AS `snow`,
				`wdir`,
				`wspd`,
				NULL AS `wpgt`,
				`pres`,
				NULL AS `tsun`,
				`coco`,
				'C' AS `priority`
			FROM `hourly_metar`
			WHERE
				`station` = '{station[0]}'
			)
		UNION ALL
			(SELECT
				`time`,
				`temp`,
				ROUND((243.04*(LN(`rhum`/100)+((17.625*`temp`)/(243.04+`temp`)))/(17.625-LN(`rhum`/100)-((17.625*`temp`)/(243.04+`temp`)))),1) AS `dwpt`,
				`rhum`,
				`prcp`,
				NULL AS `snow`,
				`wdir`,
				`wspd`,
				`wpgt`,
				`pres`,
				NULL AS `tsun`,
				`coco`,
				'D' AS `priority`
			FROM `hourly_model`
			WHERE
				`station` = '{station[0]}'
			)
		) AS `hourly_derived`
			GROUP BY
				DATE_FORMAT(`time`, '%Y %m %d %H')
			ORDER BY
				`time`
    """)

    print(df)
    exit()
