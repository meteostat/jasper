"""
Export daily bulk data

The code is licensed under the MIT license.
"""

from sys import argv
from io import BytesIO, StringIO
from gzip import GzipFile
import csv
from routines import Routine

# Configuration
STATIONS_PER_CYCLE = 11

task = Routine('export.bulk.monthly', True)

stations = task.get_stations(f'''
    SELECT
        `stations`.`id` AS `id`,
        `stations`.`tz` AS `tz`
    FROM `stations`
    WHERE
        `stations`.`id` IN (
            SELECT DISTINCT `station`
            FROM `inventory`
            WHERE
                `mode` IN ('M', 'D')
        )
''', STATIONS_PER_CYCLE)

# Export data for each weather station
for station in stations:

    result = task.read(f'''
		SET STATEMENT
			max_statement_time=60
		FOR
		SELECT
			`year`,
            `month`,
			SUBSTRING_INDEX(GROUP_CONCAT(`tavg` ORDER BY `priority`), ",", 1) AS `tavg`,
			SUBSTRING_INDEX(GROUP_CONCAT(`tmin` ORDER BY `priority`), ",", 1) AS `tmin`,
			SUBSTRING_INDEX(GROUP_CONCAT(`tmax` ORDER BY `priority`), ",", 1) AS `tmax`,
			SUBSTRING_INDEX(GROUP_CONCAT(`prcp` ORDER BY `priority`), ",", 1) AS `prcp`,
			SUBSTRING_INDEX(GROUP_CONCAT(`wdir` ORDER BY `priority`), ",", 1) AS `wdir`,
			SUBSTRING_INDEX(GROUP_CONCAT(`wspd` ORDER BY `priority`), ",", 1) AS `wspd`,
			SUBSTRING_INDEX(GROUP_CONCAT(`wpgt` ORDER BY `priority`), ",", 1) AS `wpgt`,
			SUBSTRING_INDEX(GROUP_CONCAT(`pres` ORDER BY `priority`), ",", 1) AS `pres`,
			SUBSTRING_INDEX(GROUP_CONCAT(`tsun` ORDER BY `priority`), ",", 1) AS `tsun`
		FROM (
			(SELECT
				`year`,
                `month`,
				`tavg`,
				`tmin`,
				`tmax`,
				`prcp`,
				NULL AS `wdir`,
				NULL AS `wspd`,
				NULL AS `wpgt`,
				`pres`,
				`tsun`,
				"A" AS `priority`
			FROM `monthly_global`
			WHERE
				`station` = :station
			)
        UNION ALL
			(SELECT
				YEAR(`date`) AS `year`,
                MONTH(`date`) AS `month`,
				IF(count(`daily_national`.`tavg`) < DAY(LAST_DAY(`date`)), NULL, ROUND(AVG(`daily_national`.`tavg`), 1)) AS `tavg`,
				IF(count(`daily_national`.`tmin`) < DAY(LAST_DAY(`date`)), NULL, ROUND(MIN(`daily_national`.`tmin`), 1)) AS `tmin`,
				IF(count(`daily_national`.`tmax`) < DAY(LAST_DAY(`date`)), NULL, ROUND(MAX(`daily_national`.`tmax`), 1)) AS `tmax`,
				IF(count(`daily_national`.`prcp`) < DAY(LAST_DAY(`date`)), NULL, ROUND(SUM(`daily_national`.`prcp`), 1)) AS `prcp`,
				NULL AS `wdir`,
				IF(count(`daily_national`.`wspd`) < DAY(LAST_DAY(`date`)), NULL, ROUND(AVG(`daily_national`.`wspd`), 1)) AS `wspd`,
				IF(count(`daily_national`.`wpgt`) < DAY(LAST_DAY(`date`)), NULL, ROUND(MAX(`daily_national`.`wpgt`), 1)) AS `wpgt`,
				IF(count(`daily_national`.`pres`) < DAY(LAST_DAY(`date`)), NULL, ROUND(AVG(`daily_national`.`pres`), 1)) AS `pres`,
				IF(count(`daily_national`.`tsun`) < DAY(LAST_DAY(`date`)), NULL, ROUND(SUM(`daily_national`.`tsun`), 1)) AS `tsun`,
				"B" AS `priority`
			FROM `daily_national`
			WHERE
				`station` = :station
			GROUP BY
				`station`,
				`year`,
                `month`
			)
		UNION ALL
			(SELECT
				YEAR(`date`) AS `year`,
                MONTH(`date`) AS `month`,
				IF(count(`daily_ghcn`.`tavg`) < DAY(LAST_DAY(`date`)), NULL, ROUND(AVG(`daily_ghcn`.`tavg`), 1)) AS `tavg`,
				IF(count(`daily_ghcn`.`tmin`) < DAY(LAST_DAY(`date`)), NULL, ROUND(MIN(`daily_ghcn`.`tmin`), 1)) AS `tmin`,
				IF(count(`daily_ghcn`.`tmax`) < DAY(LAST_DAY(`date`)), NULL, ROUND(MAX(`daily_ghcn`.`tmax`), 1)) AS `tmax`,
				IF(count(`daily_ghcn`.`prcp`) < DAY(LAST_DAY(`date`)), NULL, ROUND(SUM(`daily_ghcn`.`prcp`), 1)) AS `prcp`,
				IF(count(`daily_ghcn`.`wdir`) < DAY(LAST_DAY(`date`)), NULL, ROUND(AVG(`daily_ghcn`.`wdir`), 1)) AS `wdir`,
				IF(count(`daily_ghcn`.`wspd`) < DAY(LAST_DAY(`date`)), NULL, ROUND(AVG(`daily_ghcn`.`wspd`), 1)) AS `wspd`,
				IF(count(`daily_ghcn`.`wpgt`) < DAY(LAST_DAY(`date`)), NULL, ROUND(MAX(`daily_ghcn`.`wpgt`), 1)) AS `wpgt`,
				NULL AS `pres`,
				IF(count(`daily_ghcn`.`tsun`) < DAY(LAST_DAY(`date`)), NULL, ROUND(SUM(`daily_ghcn`.`tsun`),1)) AS `tsun`,
				"C" AS `priority`
			FROM `daily_ghcn`
			WHERE
				`station` = :station
			GROUP BY
				`station`,
				`year`,
                `month`
			)
		) AS `monthly_derived`
			WHERE
				`tavg` IS NOT NULL
				OR `tmin` IS NOT NULL
				OR `tmax` IS NOT NULL
				OR `prcp` IS NOT NULL
			GROUP BY
				`year`, `month`
			ORDER BY
				`year`, `month`
    ''', {
        'station': station[0]
    })

    if result.rowcount > 0:

        file = BytesIO()

        with GzipFile(fileobj=file, mode='w') as gz:
            output = StringIO()
            writer = csv.writer(output, delimiter=',')
            writer.writerows(result)
            gz.write(output.getvalue().encode())
            gz.close()
            file.seek(0)

        task.bulk_ftp.cwd(f'/monthly')
        task.bulk_ftp.storbinary(f'STOR {station[0]}.csv.gz', file)
