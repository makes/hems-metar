import os
import sys
from datetime import datetime
from datetime import timedelta
import pandas as pd
import pysolar.solar as solar

def civil_twilight(dt: datetime, lat: float, lon: float):
    """
    Returns 1 if sun is above civil twilight level (-6 degrees), else 0.

    :param datetime dt: UTC timestamp.
    :param float lat: Latitude of the location.
    :param float lon: Longitude of the location.
    :return: 0 if night, 1 if day
    :rtype: int
    """
    alt = solar.get_altitude(lat, lon, dt)
    if alt < -6:
        return 0
    else:
        return 1

def gen_twil(icao: str, lat: float, lon: float):
    start = datetime.fromisoformat('2017-12-31 23:59:00' + '+00:00')
    end = datetime.fromisoformat('2018-12-31 23:59:00' + '+00:00')

    csv_rows = ''

    t = start
    twil = civil_twilight(t, lat=lat, lon=lon)
    while t <= end:
        twil_prev = twil
        t += timedelta(minutes=1)
        twil = civil_twilight(t, lat=lat, lon=lon)
        if twil_prev == 0 and twil == 1:
            out = f"{icao},{t.strftime('%Y-%m-%d %H:%M')},1"
            csv_rows += out + '\n' # sunrise
        elif twil_prev == 1 and twil == 0:
            out = f"{icao},{t.strftime('%Y-%m-%d %H:%M')},0"
            csv_rows += out + '\n' # sunset
    return csv_rows


if __name__ == "__main__":
    geo_df = pd.read_csv('data/Saahavaintoasemat.csv', index_col='ICAO')

    icaos = ['EFHK', 'EFTU', 'EFTP', 'EFSI', 'EFOU', 'EFRO', 'EFKU', 'EFUT', 'EFLP', 'EFET']

    if len(sys.argv) > 1:
        if sys.argv[1] == 'concat':
            filename = 'output/twil_all.csv'
            out_df = pd.read_csv(f'output/twil_{icaos[0]}.csv')
            for icao in icaos[1:]:
                twil_df = pd.read_csv(f'output/twil_{icao}.csv')
                out_df = pd.concat([out_df, twil_df])
            out_df.to_csv(filename, index=False)
            exit()
        else:
            print("Invalid argument. Use 'concat' to concatenate all files.")

    task_id = os.getenv("SLURM_ARRAY_TASK_ID")
    task_id = int(task_id) if task_id is not None else None

    csv = 'icao,time,twil\n'

    if task_id is None:
        filename = 'output/twil_all.csv'
    else:
        icaos = [icaos[task_id]]
        filename = f'output/twil_{icaos[0]}.csv'

    for icao in icaos:
        print("Processing", icao)
        lat = geo_df.loc[icao, 'LAT']
        lon = geo_df.loc[icao, 'LON']
        csv += gen_twil(icao, lat, lon)

    with open(filename, "w", encoding='utf-8') as f:
        f.write(csv)
