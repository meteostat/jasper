import pandas as pd
from routines import Routine

importer = Routine('import.dwd.hourly')

result = importer.get_stations('SELECT `id`, `country` FROM `stations`', 10)
print(result)
