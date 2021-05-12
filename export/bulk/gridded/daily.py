"""
Export daily grid data

The code is licensed under the MIT license.
"""

from sys import argv
from io import BytesIO, StringIO
import datetime
import pandas as pd
from routines import Routine

# Configuration
date: datetime = datetime.date.today() - datetime.timedelta(days=1)

# Create task
task: Routine = Routine('export.bulk.gridded.daily', True)

# Export data for all weather stations
df = pd.read_sql(f'''
	SET STATEMENT
		max_statement_time=1200
	FOR
	SELECT
		`stations`.`latitude` AS `latitude`,
		`stations`.`longitude` AS `longitude`,
		SUBSTRING_INDEX(GROUP_CONCAT(`tavg` ORDER BY `priority`), ",", 1) AS `tavg`,
		SUBSTRING_INDEX(GROUP_CONCAT(`tmin` ORDER BY `priority`), ",", 1) AS `tmin`,
		SUBSTRING_INDEX(GROUP_CONCAT(`tmax` ORDER BY `priority`), ",", 1) AS `tmax`,
		SUBSTRING_INDEX(GROUP_CONCAT(`prcp` ORDER BY `priority`), ",", 1) AS `prcp`,
		SUBSTRING_INDEX(GROUP_CONCAT(`wdir` ORDER BY `priority`), ",", 1) AS `wdir`,
		SUBSTRING_INDEX(GROUP_CONCAT(`wspd` ORDER BY `priority`), ",", 1) AS `wspd`,
		SUBSTRING_INDEX(GROUP_CONCAT(`wpgt` ORDER BY `priority`), ",", 1) AS `wpgt`,
		SUBSTRING_INDEX(GROUP_CONCAT(`pres` ORDER BY `priority`), ",", 1) AS `pres`
	FROM (
		(SELECT
			`station`,
			`tavg`,
			`tmin`,
			`tmax`,
			`prcp`,
			NULL AS `wdir`,
			`wspd`,
			`wpgt`,
			`pres`,
			"A" AS `priority`
		FROM `daily_national`
		WHERE
			`date` = "{date.strftime('%y-%m-%d')}"
		)
	UNION ALL
		(SELECT
			`station`,
			`tavg`,
			`tmin`,
			`tmax`,
			`prcp`,
			`wdir`,
			`wspd`,
			`wpgt`,
			NULL AS `pres`,
			"A" AS `priority`
		FROM `daily_ghcn`
		WHERE
			`date` = "{date.strftime('%y-%m-%d')}"
		)
	UNION ALL
		(SELECT
			`hourly_synop`.`station` AS `station`,
			IF(count(`hourly_synop`.`temp`)<24, NULL, ROUND(AVG(`hourly_synop`.`temp`),1)) AS `tavg`,
			IF(count(`hourly_synop`.`temp`)<24, NULL, MIN(`hourly_synop`.`temp`)) AS `tmin`,
			IF(count(`hourly_synop`.`temp`)<24, NULL, MAX(`hourly_synop`.`temp`)) AS `tmax`,
			IF(count(`hourly_synop`.`prcp`)<24, NULL, SUM(`hourly_synop`.`prcp`)) AS `prcp`,
			IF(count(`hourly_synop`.`wdir`)<24, NULL, ROUND(DEGAVG(SUM(SIN(RADIANS(`hourly_synop`.`wdir`))), SUM(COS(RADIANS(`hourly_synop`.`wdir`)))), 1)) AS `wdir`,
			IF(count(`hourly_synop`.`wspd`)<24, NULL, ROUND(AVG(`hourly_synop`.`wspd`),1)) AS `wspd`,
			IF(count(`hourly_synop`.`wpgt`)<24, NULL, MAX(`wpgt`)) AS `wpgt`,
			IF(count(`hourly_synop`.`pres`)<24, NULL, ROUND(AVG(`hourly_synop`.`pres`),1)) AS `pres`,
			"C" AS `priority`
		FROM `hourly_synop`
		INNER JOIN `stations`
		ON
			`hourly_synop`.`station` = `stations`.`id`
		WHERE
			`hourly_synop`.`time` BETWEEN
				CONVERT_TZ("{date.strftime('%y-%m-%d')} 00:00:00", "UTC", `stations`.`tz`) AND
				CONVERT_TZ("{date.strftime('%y-%m-%d')} 23:59:59", "UTC", `stations`.`tz`)
		GROUP BY
			`station`
		)
	UNION ALL
		(SELECT
			`hourly_metar`.`station` AS `station`,
			IF(count(`hourly_metar`.`temp`)<24, NULL, ROUND(AVG(`hourly_metar`.`temp`),1)) AS `tavg`,
			IF(count(`hourly_metar`.`temp`)<24, NULL, MIN(`hourly_metar`.`temp`)) AS `tmin`,
			IF(count(`hourly_metar`.`temp`)<24, NULL, MAX(`hourly_metar`.`temp`)) AS `tmax`,
			NULL AS `prcp`,
			IF(count(`hourly_metar`.`wdir`)<24, NULL, ROUND(DEGAVG(SUM(SIN(RADIANS(`hourly_metar`.`wdir`))), SUM(COS(RADIANS(`hourly_metar`.`wdir`)))), 1)) AS `wdir`,
			IF(count(`hourly_metar`.`wspd`)<24, NULL, ROUND(AVG(`hourly_metar`.`wspd`),1)) AS `wspd`,
			NULL AS `wpgt`,
			IF(count(`hourly_metar`.`pres`)<24, NULL, ROUND(AVG(`hourly_metar`.`pres`),1)) AS `pres`,
			"D" AS `priority`
		FROM `hourly_metar`
		INNER JOIN `stations`
		ON
			`hourly_metar`.`station` = `stations`.`id`
		WHERE
			`hourly_metar`.`time` BETWEEN
				CONVERT_TZ("{date.strftime('%y-%m-%d')} 00:00:00", "UTC", `stations`.`tz`) AND
				CONVERT_TZ("{date.strftime('%y-%m-%d')} 23:59:59", "UTC", `stations`.`tz`)
		GROUP BY
			`station`
		)
	UNION ALL
		(SELECT
			`hourly_model`.`station` AS `station`,
			IF(count(`hourly_model`.`temp`)<24, NULL, ROUND(AVG(`hourly_model`.`temp`),1)) AS `tavg`,
			IF(count(`hourly_model`.`temp`)<24, NULL, MIN(`hourly_model`.`temp`)) AS `tmin`,
			IF(count(`hourly_model`.`temp`)<24, NULL, MAX(`hourly_model`.`temp`)) AS `tmax`,
			IF(count(`hourly_model`.`prcp`)<24, NULL, SUM(`hourly_model`.`prcp`)) AS `prcp`,
			IF(count(`hourly_model`.`wdir`)<24, NULL, ROUND(DEGAVG(SUM(SIN(RADIANS(`hourly_model`.`wdir`))), SUM(COS(RADIANS(`hourly_model`.`wdir`)))), 1)) AS `wdir`,
			IF(count(`hourly_model`.`wspd`)<24, NULL, ROUND(AVG(`hourly_model`.`wspd`),1)) AS `wspd`,
			IF(count(`hourly_model`.`wpgt`)<24, NULL, MAX(`hourly_model`.`wpgt`)) AS `wpgt`,
			IF(count(`hourly_model`.`pres`)<24, NULL, ROUND(AVG(`hourly_model`.`pres`),1)) AS `pres`,
			"E" AS `priority`
		FROM `hourly_model`
		INNER JOIN `stations`
		ON
			`hourly_model`.`station` = `stations`.`id`
		WHERE
			`hourly_model`.`time` BETWEEN
				CONVERT_TZ("{date.strftime('%y-%m-%d')} 00:00:00", "UTC", `stations`.`tz`) AND
				CONVERT_TZ("{date.strftime('%y-%m-%d')} 23:59:59", "UTC", `stations`.`tz`)
		GROUP BY
			`station`
		)
	) AS `daily_derived`
	INNER JOIN `stations`
	ON
		`daily_derived`.`station` = `stations`.`id`
	WHERE
		`tavg` IS NOT NULL
		OR `tmin` IS NOT NULL
		OR `tmax` IS NOT NULL
		OR `prcp` IS NOT NULL
	GROUP BY
		`station`
''', task.db)

print(df)
exit()
