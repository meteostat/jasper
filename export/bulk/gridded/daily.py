"""
Export daily grid data

The code is licensed under the MIT license.
"""

import os
from sys import argv
from pathlib import Path
import datetime
import numpy as np
import pandas as pd
import itertools
import pyproj
import verde as vd
from routines import Routine

# Configuration
delta: int = int(argv[1])
date: datetime = datetime.date.today() - datetime.timedelta(days=delta)
parameters: list = [
	'tavg',
	'tmin',
	'tmax',
	'prcp',
	'wspd',
	'pres'
]

# Create task
task: Routine = Routine('export.bulk.gridded.daily', True)

# Export data for all weather stations
raw = pd.read_sql(f'''
	SET STATEMENT
		max_statement_time=1200
	FOR
	SELECT
		ROUND(`stations`.`latitude`, 2) AS `latitude`,
		ROUND(`stations`.`longitude`, 2) AS `longitude`,
		SUBSTRING_INDEX(GROUP_CONCAT(`tavg` ORDER BY `priority`), ",", 1) AS `tavg`,
		SUBSTRING_INDEX(GROUP_CONCAT(`tmin` ORDER BY `priority`), ",", 1) AS `tmin`,
		SUBSTRING_INDEX(GROUP_CONCAT(`tmax` ORDER BY `priority`), ",", 1) AS `tmax`,
		SUBSTRING_INDEX(GROUP_CONCAT(`prcp` ORDER BY `priority`), ",", 1) AS `prcp`,
		SUBSTRING_INDEX(GROUP_CONCAT(`wspd` ORDER BY `priority`), ",", 1) AS `wspd`,
		SUBSTRING_INDEX(GROUP_CONCAT(`pres` ORDER BY `priority`), ",", 1) AS `pres`
	FROM (
{f"""
		(SELECT
			`station`,
			`tavg`,
			`tmin`,
			`tmax`,
			`prcp`,
			`wspd`,
			`pres`,
			"A" AS `priority`
		FROM `daily_national`
		WHERE
			`date` = '{date.strftime('%Y-%m-%d')}'
		)
	UNION ALL
		(SELECT
			`station`,
			`tavg`,
			`tmin`,
			`tmax`,
			`prcp`,
			`wspd`,
			NULL AS `pres`,
			"A" AS `priority`
		FROM `daily_ghcn`
		WHERE
			`date` = '{date.strftime('%Y-%m-%d')}'
		)
	UNION ALL
		(SELECT
			`hourly_national`.`station` AS `station`,
			IF(count(`hourly_national`.`temp`)<24, NULL, ROUND(AVG(`hourly_national`.`temp`), 1)) AS `tavg`,
			IF(count(`hourly_national`.`temp`)<24, NULL, MIN(`hourly_national`.`temp`)) AS `tmin`,
			IF(count(`hourly_national`.`temp`)<24, NULL, MAX(`hourly_national`.`temp`)) AS `tmax`,
			IF(count(`hourly_national`.`prcp`)<24, NULL, SUM(`hourly_national`.`prcp`)) AS `prcp`,
			IF(count(`hourly_national`.`wspd`)<24, NULL, ROUND(AVG(`hourly_national`.`wspd`), 1)) AS `wspd`,
			IF(count(`hourly_national`.`pres`)<24, NULL, ROUND(AVG(`hourly_national`.`pres`), 1)) AS `pres`,
			"B" AS `priority`
		FROM `hourly_national`
		INNER JOIN `stations`
		ON
			`hourly_national`.`station` = `stations`.`id` AND
			`hourly_national`.`time` BETWEEN
				DATE_SUB('{date.strftime('%Y-%m-%d')} 00:00:00', INTERVAL 12 HOUR) AND
				DATE_ADD('{date.strftime('%Y-%m-%d')} 23:59:59', INTERVAL 12 HOUR)
		WHERE
			`hourly_national`.`time` BETWEEN
				CONVERT_TZ('{date.strftime('%Y-%m-%d')} 00:00:00', `stations`.`tz`, "UTC") AND
				CONVERT_TZ('{date.strftime('%Y-%m-%d')} 23:59:59', `stations`.`tz`, "UTC")
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
			IF(count(`hourly_isd`.`wspd`)<24, NULL, ROUND(AVG(`hourly_isd`.`wspd`),1)) AS `wspd`,
			IF(count(`hourly_isd`.`pres`)<24, NULL, ROUND(AVG(`hourly_isd`.`pres`),1)) AS `pres`,
			"B" AS `priority`
		FROM `hourly_isd`
		INNER JOIN `stations`
		ON
			`hourly_isd`.`station` = `stations`.`id` AND
			`hourly_isd`.`time` BETWEEN
				DATE_SUB('{date.strftime('%Y-%m-%d')} 00:00:00', INTERVAL 12 HOUR) AND
				DATE_ADD('{date.strftime('%Y-%m-%d')} 23:59:59', INTERVAL 12 HOUR)
		WHERE
			`hourly_isd`.`time` BETWEEN
				CONVERT_TZ('{date.strftime('%Y-%m-%d')} 00:00:00', `stations`.`tz`, "UTC") AND
				CONVERT_TZ('{date.strftime('%Y-%m-%d')} 23:59:59', `stations`.`tz`, "UTC")
		GROUP BY
			`station`
		)
	UNION ALL
""" if delta > 7 else ''}
		(SELECT
			`hourly_synop`.`station` AS `station`,
			IF(count(`hourly_synop`.`temp`)<24, NULL, ROUND(AVG(`hourly_synop`.`temp`),1)) AS `tavg`,
			IF(count(`hourly_synop`.`temp`)<24, NULL, MIN(`hourly_synop`.`temp`)) AS `tmin`,
			IF(count(`hourly_synop`.`temp`)<24, NULL, MAX(`hourly_synop`.`temp`)) AS `tmax`,
			IF(count(`hourly_synop`.`prcp`)<24, NULL, SUM(`hourly_synop`.`prcp`)) AS `prcp`,
			IF(count(`hourly_synop`.`wspd`)<24, NULL, ROUND(AVG(`hourly_synop`.`wspd`),1)) AS `wspd`,
			IF(count(`hourly_synop`.`pres`)<24, NULL, ROUND(AVG(`hourly_synop`.`pres`),1)) AS `pres`,
			"C" AS `priority`
		FROM `hourly_synop`
		INNER JOIN `stations`
		ON
			`hourly_synop`.`station` = `stations`.`id` AND
			`hourly_synop`.`time` BETWEEN
				DATE_SUB("{date.strftime('%Y-%m-%d')} 00:00:00", INTERVAL 12 HOUR) AND
				DATE_ADD("{date.strftime('%Y-%m-%d')} 23:59:59", INTERVAL 12 HOUR)
		WHERE
			`hourly_synop`.`time` BETWEEN
				CONVERT_TZ("{date.strftime('%Y-%m-%d')} 00:00:00", `stations`.`tz`, "UTC") AND
				CONVERT_TZ("{date.strftime('%Y-%m-%d')} 23:59:59", `stations`.`tz`, "UTC")
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
			IF(count(`hourly_metar`.`wspd`)<24, NULL, ROUND(AVG(`hourly_metar`.`wspd`),1)) AS `wspd`,
			IF(count(`hourly_metar`.`pres`)<24, NULL, ROUND(AVG(`hourly_metar`.`pres`),1)) AS `pres`,
			"D" AS `priority`
		FROM `hourly_metar`
		INNER JOIN `stations`
		ON
			`hourly_metar`.`station` = `stations`.`id` AND
			`hourly_metar`.`time` BETWEEN
				DATE_SUB("{date.strftime('%Y-%m-%d')} 00:00:00", INTERVAL 12 HOUR) AND
				DATE_ADD("{date.strftime('%Y-%m-%d')} 23:59:59", INTERVAL 12 HOUR)
		WHERE
			`hourly_metar`.`time` BETWEEN
				CONVERT_TZ("{date.strftime('%Y-%m-%d')} 00:00:00", `stations`.`tz`, "UTC") AND
				CONVERT_TZ("{date.strftime('%Y-%m-%d')} 23:59:59", `stations`.`tz`, "UTC")
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
			IF(count(`hourly_model`.`wspd`)<24, NULL, ROUND(AVG(`hourly_model`.`wspd`),1)) AS `wspd`,
			IF(count(`hourly_model`.`pres`)<24, NULL, ROUND(AVG(`hourly_model`.`pres`),1)) AS `pres`,
			"E" AS `priority`
		FROM `hourly_model`
		INNER JOIN `stations`
		ON
			`hourly_model`.`station` = `stations`.`id` AND
			`hourly_model`.`time` BETWEEN
				DATE_SUB("{date.strftime('%Y-%m-%d')} 00:00:00", INTERVAL 12 HOUR) AND
				DATE_ADD("{date.strftime('%Y-%m-%d')} 23:59:59", INTERVAL 12 HOUR)
		WHERE
			`hourly_model`.`time` BETWEEN
				CONVERT_TZ("{date.strftime('%Y-%m-%d')} 00:00:00", `stations`.`tz`, "UTC") AND
				CONVERT_TZ("{date.strftime('%Y-%m-%d')} 23:59:59", `stations`.`tz`, "UTC")
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

# Clean DataFrame
raw = raw.drop(raw[raw.latitude < -89.9].index)
raw = raw.drop(raw[raw.latitude > 89.9].index)
raw = raw.drop(raw[raw.longitude < -179.9].index)
raw = raw.drop(raw[raw.longitude > 179.9].index)

if len(raw.index):
	for parameter in parameters:
		# Create subset
		df = raw[['latitude', 'longitude', parameter]]

		# Remove NaN values
		df = df[df[parameter].notna()]

		if len(df.index) > 250:
			# Convert to float
			df = df.astype(np.float64)

			# Use Mercator projection because Spline is a Cartesian gridder
			projection = pyproj.Proj(proj="merc", lat_ts=df.latitude.mean())
			proj_coords = projection(df.longitude.values, df.latitude.values)
			region = vd.get_region((df.longitude, df.latitude))

			# The desired grid spacing in degrees (converted to meters using 1 degree approx. 111km)
			spacing = 1

			# Set up gridder
			grd = vd.ScipyGridder(method="cubic").fit(proj_coords, df[parameter])

			# Create grid
			grid = grd.grid(
			    region=region,
			    spacing=spacing,
			    projection=projection,
			    dims=["lat", "lon"],
			    data_names="value"
			)

			# Mask grid points that are too far from the given data points
			mask = vd.distance_mask(
			    (df.longitude, df.latitude),
			    maxdist=0.5 * 111e3,
			    coordinates=vd.grid_coordinates(region, spacing=spacing),
			    projection=projection
			)

			# Export grid as NetCDF4
			grid = grid.where(mask)
			filename = f'{Path(__file__).parent}/temp.nc'
			grid.to_netcdf(filename)

			# Transfer to bulk server
			file = open(filename, 'rb')
			task.bulk_ftp.cwd(f'/gridded/daily/{parameter}')
			task.bulk_ftp.storbinary(f'STOR {date.strftime("%Y-%m-%d")}.nc', file)

			# Remove temp file
			if os.path.exists(filename):
			    os.remove(filename)
