import pandas as pd
from routines import Importer

routine = Importer('DWD.HOURLY', 'hourly_national')

df = pd.read_sql('SELECT `id`, `country` FROM `stations` LIMIT 10', routine.db)

print(df)
