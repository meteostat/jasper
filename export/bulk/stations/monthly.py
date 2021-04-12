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
SCOPE = argv[1]
STATIONS_PER_CYCLE = 11

task = Routine('export.bulk.monthly.' + SCOPE.lower(), True)

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
                `mode` IN {"('M', 'D', 'H', 'P')" if SCOPE == 'full' else "('M', 'D', 'H')"}
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
            SUBSTRING_INDEX(GROUP_CONCAT(`snow` ORDER BY `priority`), ",", 1) AS `snow`,
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
				NULL AS `snow`,
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
                IF(count(`daily_derived`.`tavg`) < DAY(LAST_DAY(`date`)), NULL, ROUND(AVG(`daily_derived`.`tavg`), 1)) AS `tavg`,
                IF(count(`daily_derived`.`tmin`) < DAY(LAST_DAY(`date`)), NULL, ROUND(AVG(`daily_derived`.`tmin`), 1)) AS `tmin`,
                IF(count(`daily_derived`.`tmax`) < DAY(LAST_DAY(`date`)), NULL, ROUND(AVG(`daily_derived`.`tmax`), 1)) AS `tmax`,
                IF(count(`daily_derived`.`prcp`) < DAY(LAST_DAY(`date`)), NULL, ROUND(SUM(`daily_derived`.`prcp`), 1)) AS `prcp`,
                IF(count(`daily_derived`.`snow`) < DAY(LAST_DAY(`date`)), NULL, ROUND(MAX(`daily_derived`.`snow`), 1)) AS `snow`,
                IF(count(`daily_derived`.`wdir`) < DAY(LAST_DAY(`date`)), NULL, ROUND(AVG(`daily_derived`.`wdir`), 1)) AS `wdir`,
                IF(count(`daily_derived`.`wspd`) < DAY(LAST_DAY(`date`)), NULL, ROUND(AVG(`daily_derived`.`wspd`), 1)) AS `wspd`,
                IF(count(`daily_derived`.`wpgt`) < DAY(LAST_DAY(`date`)), NULL, ROUND(MAX(`daily_derived`.`wpgt`), 1)) AS `wpgt`,
                IF(count(`daily_derived`.`pres`) < DAY(LAST_DAY(`date`)), NULL, ROUND(AVG(`daily_derived`.`pres`), 1)) AS `pres`,
                IF(count(`daily_derived`.`tsun`) < DAY(LAST_DAY(`date`)), NULL, ROUND(SUM(`daily_derived`.`tsun`), 1)) AS `tsun`,
                "B" AS `priority`
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
    				"A" AS `priority`
    			FROM `daily_ghcn`
    			WHERE
    				`station` = :station
    			)
    		UNION ALL
    			(SELECT
    				DATE(CONVERT_TZ(`hourly_national`.`time`, "UTC", :timezone)) AS `date`,
    				IF(count(`hourly_national`.`temp`)<24, NULL, ROUND(AVG(`hourly_national`.`temp`),1)) AS `tavg`,
    				IF(count(`hourly_national`.`temp`)<24, NULL, MIN(`hourly_national`.`temp`)) AS `tmin`,
    				IF(count(`hourly_national`.`temp`)<24, NULL, MAX(`hourly_national`.`temp`)) AS `tmax`,
    				IF(count(`hourly_national`.`prcp`)<24, NULL, SUM(`hourly_national`.`prcp`)) AS `prcp`,
    				NULL AS `snow`,
    				IF(count(`hourly_national`.`wdir`)<24, NULL, ROUND(AVG(`hourly_national`.`wdir`),1)) AS `wdir`,
    				IF(count(`hourly_national`.`wspd`)<24, NULL, ROUND(AVG(`hourly_national`.`wspd`),1)) AS `wspd`,
    				NULL AS `wpgt`,
    				IF(count(`hourly_national`.`pres`)<24, NULL, ROUND(AVG(`hourly_national`.`pres`),1)) AS `pres`,
    				NULL AS `tsun`,
    				"B" AS `priority`
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
    				IF(count(`hourly_isd`.`wdir`)<24, NULL, ROUND(AVG(`hourly_isd`.`wdir`),1)) AS `wdir`,
    				IF(count(`hourly_isd`.`wspd`)<24, NULL, ROUND(AVG(`hourly_isd`.`wspd`),1)) AS `wspd`,
    				NULL AS `wpgt`,
    				IF(count(`hourly_isd`.`pres`)<24, NULL, ROUND(AVG(`hourly_isd`.`pres`),1)) AS `pres`,
    				NULL AS `tsun`,
    				"B" AS `priority`
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
    				IF(count(`hourly_synop`.`wdir`)<24, NULL, ROUND(AVG(`hourly_synop`.`wdir`),1)) AS `wdir`,
    				IF(count(`hourly_synop`.`wspd`)<24, NULL, ROUND(AVG(`hourly_synop`.`wspd`),1)) AS `wspd`,
    				IF(count(`hourly_synop`.`wpgt`)<24, NULL, MAX(`wpgt`)) AS `wpgt`,
    				IF(count(`hourly_synop`.`pres`)<24, NULL, ROUND(AVG(`hourly_synop`.`pres`),1)) AS `pres`,
    				NULL AS `tsun`,
    				"C" AS `priority`
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
    				IF(count(`hourly_metar`.`wdir`)<24, NULL, ROUND(AVG(`hourly_metar`.`wdir`),1)) AS `wdir`,
    				IF(count(`hourly_metar`.`wspd`)<24, NULL, ROUND(AVG(`hourly_metar`.`wspd`),1)) AS `wspd`,
    				NULL AS `wpgt`,
    				IF(count(`hourly_metar`.`pres`)<24, NULL, ROUND(AVG(`hourly_metar`.`pres`),1)) AS `pres`,
    				NULL AS `tsun`,
    				"D" AS `priority`
    			FROM `hourly_metar`
    			WHERE
    				`hourly_metar`.`station` = :station
    			GROUP BY
    				`station`,
    				`date`
    			)
        {f"""
    		UNION ALL
    			(SELECT
    				DATE(CONVERT_TZ(`hourly_model`.`time`, "UTC", :timezone)) AS `date`,
    				IF(count(`hourly_model`.`temp`)<24, NULL, ROUND(AVG(`hourly_model`.`temp`),1)) AS `tavg`,
    				IF(count(`hourly_model`.`temp`)<24, NULL, MIN(`hourly_model`.`temp`)) AS `tmin`,
    				IF(count(`hourly_model`.`temp`)<24, NULL, MAX(`hourly_model`.`temp`)) AS `tmax`,
    				IF(count(`hourly_model`.`prcp`)<24, NULL, SUM(`hourly_model`.`prcp`)) AS `prcp`,
    				NULL AS `snow`,
    				IF(count(`hourly_model`.`wdir`)<24, NULL, ROUND(AVG(`hourly_model`.`wdir`),1)) AS `wdir`,
    				IF(count(`hourly_model`.`wspd`)<24, NULL, ROUND(AVG(`hourly_model`.`wspd`),1)) AS `wspd`,
    				IF(count(`hourly_model`.`wpgt`)<24, NULL, MAX(`hourly_model`.`wpgt`)) AS `wpgt`,
    				IF(count(`hourly_model`.`pres`)<24, NULL, ROUND(AVG(`hourly_model`.`pres`),1)) AS `pres`,
    				NULL AS `tsun`,
    				"E" AS `priority`
    			FROM `hourly_model`
    			WHERE
    				`hourly_model`.`station` = :station
    			GROUP BY
    				`station`,
    				`date`
    			)
        """ if SCOPE == 'full' else ''}
    		) AS `daily_derived`
    			GROUP BY
    				`date`
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
        'station': station[0],
        'timezone': station[1]
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

        task.bulk_ftp.cwd(f'/monthly/{SCOPE}')
        task.bulk_ftp.storbinary(f'STOR {station[0]}.csv.gz', file)
