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
