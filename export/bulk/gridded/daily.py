"""
Export daily grid data

The code is licensed under the MIT license.
"""

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
date: datetime = datetime.date.today() - datetime.timedelta(days=7)
parameters: list = [
	'tavg',
	'tmin',
	'tmax',
	'prcp',
	'wdir',
	'wspd',
	'wpgt',
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

			# Tuning
			dampings = [None, 1e-4, 1e-3, 1e-2]
			mindists = [5e3, 10e3, 50e3, 100e3]

			# Use itertools to create a list with all combinations of parameters to test
			parameter_sets = [
			    dict(damping=combo[0], mindist=combo[1])
			    for combo in itertools.product(dampings, mindists)
			]

			# Loop over the combinations and collect the scores for each parameter set
			spline = vd.Spline()
			scores = []
			for params in parameter_sets:
			    spline.set_params(**params)
			    score = np.mean(vd.cross_val_score(spline, proj_coords, df[parameter]))
			    scores.append(score)

			# The largest score will yield the best parameter combination.
			best = np.argmax(scores)

			# Cross-validated gridders
			spline = vd.SplineCV(
			    dampings=dampings,
			    mindists=mindists,
			)

			# Calling :meth:`~verde.SplineCV.fit` will run a grid search over all parameter
			# combinations to find the one that maximizes the cross-validation score.
			spline.fit(proj_coords, df[parameter])

			# Cross-validated gridder
			grid = spline.grid(
			    region=region,
			    spacing=spacing,
			    projection=projection,
			    dims=["lat", "lon"],
			    data_names="value",
			)

			spline = vd.SplineCV(dampings=dampings, mindists=mindists, delayed=True)
			spline.fit(proj_coords, df[parameter])

			# Plot grids side-by-side:
			mask = vd.distance_mask(
			    (df.longitude, df.latitude),
			    maxdist=3 * spacing * 111e3,
			    coordinates=vd.grid_coordinates(region, spacing=spacing),
			    projection=projection,
			)

			# Export grid as NetCDF4
			grid = grid.where(mask)
			grid.to_netcdf(f'{Path(__file__).parent}/{parameter}.nc')
			exit()
