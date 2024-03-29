"""
Export daily bulk data (legacy)

Observation data needs to be exported in a legacy script
Also, review time zones and flags on .sql files

The code is licensed under the MIT license.
"""

from jasper import Jasper
from jasper.helpers import get_stations
from jasper.actions import export_csv


# General configuration
STATIONS_PER_CYCLE = 10

# Create Jasper instance
jsp = Jasper("export.bulk.daily.legacy")

# Get weather station(s)
stations = get_stations(
    jsp,
    """
		SELECT
				`stations`.`id` AS `id`,
				`stations`.`tz` AS `tz`
		FROM `stations`
		WHERE
				`stations`.`id` IN (
						SELECT DISTINCT `station`
						FROM `inventory`
						WHERE
								`mode` IN ('D', 'H')
				)
""",
    STATIONS_PER_CYCLE,
)

# Export data for each weather station
for station in stations:
    result = jsp.query(
        """
			SET STATEMENT
				max_statement_time=60
			FOR
			SELECT
				`date`,
				SUBSTRING_INDEX(GROUP_CONCAT(`tavg` ORDER BY `priority` ASC), ",", 1) AS `tavg`,
				SUBSTRING_INDEX(GROUP_CONCAT(`tmin` ORDER BY `priority` ASC), ",", 1) AS `tmin`,
				SUBSTRING_INDEX(GROUP_CONCAT(`tmax` ORDER BY `priority` ASC), ",", 1) AS `tmax`,
				SUBSTRING_INDEX(GROUP_CONCAT(`prcp` ORDER BY `priority` ASC), ",", 1) AS `prcp`,
				SUBSTRING_INDEX(GROUP_CONCAT(`snow` ORDER BY `priority` ASC), ",", 1) AS `snow`,
				SUBSTRING_INDEX(GROUP_CONCAT(`wdir` ORDER BY `priority` ASC), ",", 1) AS `wdir`,
				SUBSTRING_INDEX(GROUP_CONCAT(`wspd` ORDER BY `priority` ASC), ",", 1) AS `wspd`,
				SUBSTRING_INDEX(GROUP_CONCAT(`wpgt` ORDER BY `priority` ASC), ",", 1) AS `wpgt`,
				SUBSTRING_INDEX(GROUP_CONCAT(`pres` ORDER BY `priority` ASC), ",", 1) AS `pres`,
				SUBSTRING_INDEX(GROUP_CONCAT(`tsun` ORDER BY `priority` ASC), ",", 1) AS `tsun`
			FROM (
				(SELECT
					`date`,
					`tavg`,
					`tmin`,
					`tmax`,
					`prcp`,
					`snow`,
					NULL AS `wdir`,
					`wspd`,
					`wpgt`,
					`pres`,
					`tsun`,
					"A" AS `priority`
				FROM `daily_national`
				WHERE
					`station` = :station
				)
			UNION ALL
				(SELECT
					`date`,
					`tavg`,
					`tmin`,
					`tmax`,
					`prcp`,
					`snow`,
					`wdir`,
					`wspd`,
					`wpgt`,
					NULL AS `pres`,
					`tsun`,
					"B" AS `priority`
				FROM `daily_ghcn`
				WHERE
					`station` = :station
				)
			UNION ALL
				(SELECT
					DATE(CONVERT_TZ(`hourly_national`.`time`, "UTC", :timezone)) AS `date`,
					IF(count(`hourly_national`.`temp`)<24, NULL, ROUND(AVG(`hourly_national`.`temp`), 1)) AS `tavg`,
					IF(count(`hourly_national`.`temp`)<24, NULL, MIN(`hourly_national`.`temp`)) AS `tmin`,
					IF(count(`hourly_national`.`temp`)<24, NULL, MAX(`hourly_national`.`temp`)) AS `tmax`,
					IF(count(`hourly_national`.`prcp`)<24, NULL, SUM(`hourly_national`.`prcp`)) AS `prcp`,
					NULL AS `snow`,
					IF(count(`hourly_national`.`wdir`)<24, NULL, ROUND(DEGAVG(SUM(SIN(RADIANS(`hourly_national`.`wdir`))), SUM(COS(RADIANS(`hourly_national`.`wdir`)))), 1)) AS `wdir`,
					IF(count(`hourly_national`.`wspd`)<24, NULL, ROUND(AVG(`hourly_national`.`wspd`), 1)) AS `wspd`,
					NULL AS `wpgt`,
					IF(count(`hourly_national`.`pres`)<24, NULL, ROUND(AVG(`hourly_national`.`pres`), 1)) AS `pres`,
					NULL AS `tsun`,
					"C" AS `priority`
				FROM `hourly_national`
				WHERE
					`hourly_national`.`station` = :station
				GROUP BY
					`station`,
					`date`
				)
			UNION ALL
				(SELECT
					DATE(CONVERT_TZ(`hourly_isd`.`time`, "UTC", :timezone)) AS `date`,
					IF(count(`hourly_isd`.`temp`)<24, NULL, ROUND(AVG(`hourly_isd`.`temp`),1)) AS `tavg`,
					IF(count(`hourly_isd`.`temp`)<24, NULL, MIN(`hourly_isd`.`temp`)) AS `tmin`,
					IF(count(`hourly_isd`.`temp`)<24, NULL, MAX(`hourly_isd`.`temp`)) AS `tmax`,
					IF(count(`hourly_isd`.`prcp`)<24, NULL, SUM(`hourly_isd`.`prcp`)) AS `prcp`,
					NULL AS `snow`,
					IF(count(`hourly_isd`.`wdir`)<24, NULL, ROUND(DEGAVG(SUM(SIN(RADIANS(`hourly_isd`.`wdir`))), SUM(COS(RADIANS(`hourly_isd`.`wdir`)))), 1)) AS `wdir`,
					IF(count(`hourly_isd`.`wspd`)<24, NULL, ROUND(AVG(`hourly_isd`.`wspd`),1)) AS `wspd`,
					NULL AS `wpgt`,
					IF(count(`hourly_isd`.`pres`)<24, NULL, ROUND(AVG(`hourly_isd`.`pres`),1)) AS `pres`,
					NULL AS `tsun`,
					"D" AS `priority`
				FROM `hourly_isd`
				WHERE
					`hourly_isd`.`station` = :station
				GROUP BY
					`station`,
					`date`
				)
			UNION ALL
				(SELECT
					DATE(CONVERT_TZ(`hourly_synop`.`time`, "UTC", :timezone)) AS `date`,
					IF(count(`hourly_synop`.`temp`)<24, NULL, ROUND(AVG(`hourly_synop`.`temp`),1)) AS `tavg`,
					IF(count(`hourly_synop`.`temp`)<24, NULL, MIN(`hourly_synop`.`temp`)) AS `tmin`,
					IF(count(`hourly_synop`.`temp`)<24, NULL, MAX(`hourly_synop`.`temp`)) AS `tmax`,
					IF(count(`hourly_synop`.`prcp`)<24, NULL, SUM(`hourly_synop`.`prcp`)) AS `prcp`,
					IF(count(`hourly_synop`.`snow`)<24, NULL, MAX(`hourly_synop`.`snow`)) AS `snow`,
					IF(count(`hourly_synop`.`wdir`)<24, NULL, ROUND(DEGAVG(SUM(SIN(RADIANS(`hourly_synop`.`wdir`))), SUM(COS(RADIANS(`hourly_synop`.`wdir`)))), 1)) AS `wdir`,
					IF(count(`hourly_synop`.`wspd`)<24, NULL, ROUND(AVG(`hourly_synop`.`wspd`),1)) AS `wspd`,
					IF(count(`hourly_synop`.`wpgt`)<24, NULL, MAX(`wpgt`)) AS `wpgt`,
					IF(count(`hourly_synop`.`pres`)<24, NULL, ROUND(AVG(`hourly_synop`.`pres`),1)) AS `pres`,
					NULL AS `tsun`,
					"E" AS `priority`
				FROM `hourly_synop`
				WHERE
					`hourly_synop`.`station` = :station
				GROUP BY
					`station`,
					`date`
				)
			UNION ALL
				(SELECT
					DATE(CONVERT_TZ(`hourly_metar`.`time`, "UTC", :timezone)) AS `date`,
					IF(count(`hourly_metar`.`temp`)<24, NULL, ROUND(AVG(`hourly_metar`.`temp`),1)) AS `tavg`,
					IF(count(`hourly_metar`.`temp`)<24, NULL, MIN(`hourly_metar`.`temp`)) AS `tmin`,
					IF(count(`hourly_metar`.`temp`)<24, NULL, MAX(`hourly_metar`.`temp`)) AS `tmax`,
					NULL AS `prcp`,
					NULL AS `snow`,
					IF(count(`hourly_metar`.`wdir`)<24, NULL, ROUND(DEGAVG(SUM(SIN(RADIANS(`hourly_metar`.`wdir`))), SUM(COS(RADIANS(`hourly_metar`.`wdir`)))), 1)) AS `wdir`,
					IF(count(`hourly_metar`.`wspd`)<24, NULL, ROUND(AVG(`hourly_metar`.`wspd`),1)) AS `wspd`,
					NULL AS `wpgt`,
					IF(count(`hourly_metar`.`pres`)<24, NULL, ROUND(AVG(`hourly_metar`.`pres`),1)) AS `pres`,
					NULL AS `tsun`,
					"F" AS `priority`
				FROM `hourly_metar`
				WHERE
					`hourly_metar`.`station` = :station
				GROUP BY
					`station`,
					`date`
				)
			) AS `daily_derived`
			WHERE
							(
						`tavg` IS NOT NULL OR
						`tmin` IS NOT NULL OR
						`tmax` IS NOT NULL OR
						`prcp` IS NOT NULL
							) AND
							`date` <= DATE_ADD(CURRENT_DATE(), INTERVAL 10 DAY)
			GROUP BY
				`date`
			ORDER BY
				`date`
		""",
        {"station": station[0], "timezone": station[1]},
    )

    if result.rowcount > 0:
        # Fetch data
        data = result.fetchall()

        # Export data dump
        export_csv(
            jsp, list(map(lambda d: d[:11], data)), f"/daily/obs/{station[0]}.csv.gz"
        )

# Close Jasper instance
jsp.close()
