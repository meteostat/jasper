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
		SUBSTRING_INDEX(GROUP_CONCAT(`snow` ORDER BY `priority`), ",", 1) AS `snow`,
		SUBSTRING_INDEX(GROUP_CONCAT(`wdir` ORDER BY `priority`), ",", 1) AS `wdir`,
		SUBSTRING_INDEX(GROUP_CONCAT(`wspd` ORDER BY `priority`), ",", 1) AS `wspd`,
		SUBSTRING_INDEX(GROUP_CONCAT(`wpgt` ORDER BY `priority`), ",", 1) AS `wpgt`,
		SUBSTRING_INDEX(GROUP_CONCAT(`pres` ORDER BY `priority`), ",", 1) AS `pres`,
		SUBSTRING_INDEX(GROUP_CONCAT(`tsun` ORDER BY `priority`), ",", 1) AS `tsun`
	FROM (
		(SELECT
			`station`,
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
			`date` LIKE "{date.strftime('%y-%m-%d')}"
		)
	UNION ALL
		(SELECT
			`station`,
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
			"A" AS `priority`
		FROM `daily_ghcn`
		WHERE
			`date` LIKE "{date.strftime('%y-%m-%d')}"
		)
	UNION ALL
		(SELECT
			`hourly_national`.`station` AS `station`,
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
			"B" AS `priority`
		FROM `hourly_national`
		INNER JOIN `stations`
		ON
			`hourly_national`.`station` = `stations`.`id`
		WHERE
			DATE(CONVERT_TZ(`hourly_national`.`time`, "UTC", `stations`.`tz`)) LIKE "{date.strftime('%y-%m-%d')}%"
		GROUP BY
			`station`
		)
	UNION ALL
		(SELECT
			`hourly_isd`.`station` AS `station`,
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
			"B" AS `priority`
		FROM `hourly_isd`
		INNER JOIN `stations`
		ON
			`hourly_isd`.`station` = `stations`.`id`
		WHERE
			DATE(CONVERT_TZ(`hourly_isd`.`time`, "UTC", `stations`.`tz`)) LIKE "{date.strftime('%y-%m-%d')}%"
		GROUP BY
			`station`
		)
	UNION ALL
		(SELECT
			`hourly_synop`.`station` AS `station`,
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
			"C" AS `priority`
		FROM `hourly_synop`
		INNER JOIN `stations`
		ON
			`hourly_synop`.`station` = `stations`.`id`
		WHERE
			DATE(CONVERT_TZ(`hourly_synop`.`time`, "UTC", `stations`.`tz`)) LIKE "{date.strftime('%y-%m-%d')}%"
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
			NULL AS `snow`,
			IF(count(`hourly_metar`.`wdir`)<24, NULL, ROUND(DEGAVG(SUM(SIN(RADIANS(`hourly_metar`.`wdir`))), SUM(COS(RADIANS(`hourly_metar`.`wdir`)))), 1)) AS `wdir`,
			IF(count(`hourly_metar`.`wspd`)<24, NULL, ROUND(AVG(`hourly_metar`.`wspd`),1)) AS `wspd`,
			NULL AS `wpgt`,
			IF(count(`hourly_metar`.`pres`)<24, NULL, ROUND(AVG(`hourly_metar`.`pres`),1)) AS `pres`,
			NULL AS `tsun`,
			"D" AS `priority`
		FROM `hourly_metar`
		INNER JOIN `stations`
		ON
			`hourly_metar`.`station` = `stations`.`id`
		WHERE
			DATE(CONVERT_TZ(`hourly_metar`.`time`, "UTC", `stations`.`tz`)) LIKE "{date.strftime('%y-%m-%d')}%"
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
			NULL AS `snow`,
			IF(count(`hourly_model`.`wdir`)<24, NULL, ROUND(DEGAVG(SUM(SIN(RADIANS(`hourly_model`.`wdir`))), SUM(COS(RADIANS(`hourly_model`.`wdir`)))), 1)) AS `wdir` AS `wdir`,
			IF(count(`hourly_model`.`wspd`)<24, NULL, ROUND(AVG(`hourly_model`.`wspd`),1)) AS `wspd`,
			IF(count(`hourly_model`.`wpgt`)<24, NULL, MAX(`hourly_model`.`wpgt`)) AS `wpgt`,
			IF(count(`hourly_model`.`pres`)<24, NULL, ROUND(AVG(`hourly_model`.`pres`),1)) AS `pres`,
			NULL AS `tsun`,
			"E" AS `priority`
		FROM `hourly_model`
		INNER JOIN `stations`
		ON
			`hourly_model`.`station` = `stations`.`id`
		WHERE
			DATE(CONVERT_TZ(`hourly_model`.`time`, "UTC", `stations`.`tz`)) LIKE "{date.strftime('%y-%m-%d')}%"
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
