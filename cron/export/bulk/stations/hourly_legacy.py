"""
Export hourly bulk data (legacy)

The code is licensed under the MIT license.
"""

from sys import argv
from datetime import datetime
from jasper import Jasper
from jasper.helpers import get_stations
from jasper.actions import export_csv

# Task mode
# 'all' or 'recent'
MODE = argv[1]

# General configuration
STATIONS_PER_CYCLE =  8 if MODE == "recent" else 1

# Create Jasper instance
jsp = Jasper(f"export.bulk.hourly.legacy.{MODE}")


def write_dump(data: list, station: str, year: int = None) -> None:
    """
    Convert DataFrame to CSV and export to bulk server
    """
    # The file path
    path = "/hourly/obs"

    # Filter rows by year if set
    if year is not None:
        path += f"/{year}"
        data = list(filter(lambda row: int(row[0].year) == year, data))

    # Export data dump
    export_csv(jsp, list(map(lambda d: d[:13], data)), f"{path}/{station}.csv.gz")


# Get weather stations
stations = get_stations(
    jsp,
    """
		SELECT
				`stations`.`id` AS `id`
		FROM `stations`
		WHERE
				`stations`.`id` IN (
						SELECT DISTINCT `station`
						FROM `inventory`
						WHERE
								`mode` IN ('H')
				)
""",
    STATIONS_PER_CYCLE,
)

# Start & end year
now = datetime.now()
start_year = now.year - 1 if MODE == "recent" else 1890
end_year = now.year + 1

# Export data for each weather station
for station in stations:
    result = jsp.query(
        f"""
			SET STATEMENT
				max_statement_time=90
			FOR
			SELECT
				DATE(MIN(`time`)) AS `date`,
				DATE_FORMAT(MIN(`time`), '%H') AS `hour`,
				SUBSTRING_INDEX(GROUP_CONCAT(`temp` ORDER BY `priority` ASC), ',', 1) AS `temp`,
				SUBSTRING_INDEX(GROUP_CONCAT(`dwpt` ORDER BY `priority` ASC), ',', 1) AS `dwpt`,
				SUBSTRING_INDEX(GROUP_CONCAT(`rhum` ORDER BY `priority` ASC), ',', 1) AS `rhum`,
				SUBSTRING_INDEX(GROUP_CONCAT(`prcp` ORDER BY `priority` ASC), ',', 1) AS `prcp`,
				SUBSTRING_INDEX(GROUP_CONCAT(`snow` ORDER BY `priority` ASC), ',', 1) AS `snow`,
				SUBSTRING_INDEX(GROUP_CONCAT(`wdir` ORDER BY `priority` ASC), ',', 1) AS `wdir`,
				SUBSTRING_INDEX(GROUP_CONCAT(`wspd` ORDER BY `priority` ASC), ',', 1) AS `wspd`,
				SUBSTRING_INDEX(GROUP_CONCAT(`wpgt` ORDER BY `priority` ASC), ',', 1) AS `wpgt`,
				SUBSTRING_INDEX(GROUP_CONCAT(`pres` ORDER BY `priority` ASC), ',', 1) AS `pres`,
				SUBSTRING_INDEX(GROUP_CONCAT(`tsun` ORDER BY `priority` ASC), ',', 1) AS `tsun`,
				SUBSTRING_INDEX(GROUP_CONCAT(`coco` ORDER BY `priority` ASC), ',', 1) AS `coco`
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
					`station` = :station
									{f'AND `time` BETWEEN "{start_year}-01-01 00:00:00" AND "{end_year}-12-31 23:59:59"' if MODE == 'recent' else ''}
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
					'B' AS `priority`
				FROM `hourly_isd`
				WHERE
					`station` = :station
									{f'AND `time` BETWEEN "{start_year}-01-01 00:00:00" AND "{end_year}-12-31 23:59:59"' if MODE == 'recent' else ''}
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
					'C' AS `priority`
				FROM `hourly_synop`
				WHERE
					`station` = :station
									{f'AND `time` BETWEEN "{start_year}-01-01 00:00:00" AND "{end_year}-12-31 23:59:59"' if MODE == 'recent' else ''}
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
					'D' AS `priority`
				FROM `hourly_metar`
				WHERE
					`station` = :station
									{f'AND `time` BETWEEN "{start_year}-01-01 00:00:00" AND "{end_year}-12-31 23:59:59"' if MODE == 'recent' else ''}
				)
			) AS `hourly_derived`
					WHERE
							`time` <= DATE_ADD(NOW(), INTERVAL 10 DAY)
			GROUP BY
				DATE_FORMAT(`time`, '%Y %m %d %H')
			ORDER BY
				`time`
		""",
        {"station": station[0]},
    )

    if result.rowcount > 0:
        # Fetch data
        data = result.fetchall()

        # Write all data
        if MODE == "all":
            write_dump(data, station[0])

        # Write annually
        first_year = int(data[0][0].year)
        last_year = int(data[-1][0].year)

        for year in range(first_year, last_year + 1):
            write_dump(data, station[0], year)

# Close Jasper instance
jsp.close()
