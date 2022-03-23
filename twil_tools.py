from datetime import datetime
from datetime import timedelta
import pandas as pd
#import pysolar.solar as solar

def convert_twilight_data(filename):
    """
    Fixes and converts the broken twiligt AIP csv.
    """
    df = pd.read_csv(filename, sep=';', usecols=['AD', 'SQL_TWIL_FM', 'SQL_TWIL_TO'], na_values=['-'])
    icaos = df['AD'].unique()
    twil_data = []
    for icao in icaos:
        df_icao = pd.DataFrame(df[df['AD'] == icao])
        df_icao['SQL_TWIL_FM'] = pd.to_datetime(df_icao['SQL_TWIL_FM'], errors='coerce')
        df_icao['SQL_TWIL_TO'] = pd.to_datetime(df_icao['SQL_TWIL_TO'], errors='coerce')
        for i in df_icao.index:
            if pd.isnull(df_icao.loc[i, 'SQL_TWIL_FM']) or pd.isnull(df_icao.loc[i, 'SQL_TWIL_TO']):
                continue
            if df_icao.loc[i, 'SQL_TWIL_FM'] > df_icao.loc[i, 'SQL_TWIL_TO']:
                df_icao.loc[i, 'SQL_TWIL_FM'] -= pd.Timedelta(days=1)
        twil_from = pd.DataFrame(df_icao.SQL_TWIL_FM)
        twil_from.dropna(inplace=True)
        twil_from['TWIL'] = 'FROM'
        twil_from.set_index('SQL_TWIL_FM', inplace=True)
        twil_to = pd.DataFrame(df_icao.SQL_TWIL_TO)
        twil_to.dropna(inplace=True)
        twil_to['TWIL'] = 'TO'
        twil_to.set_index('SQL_TWIL_TO', inplace=True)
        data = twil_from.merge(twil_to, how='outer', left_index=True, right_index=True)
        prev_val = None
        for idx in data.index:
            val = data.loc[idx, 'TWIL_x']
            if prev_val is None:
                prev_val = val
                continue
            if prev_val == 'FROM' and val == 'FROM':
                print(f"ERROR in {icao} at {idx}")
            if pd.isnull(prev_val) and pd.isnull(val):
                print(f"ERROR in {icao} at {idx}")
            prev_val = val
        twil_data.append(pd.DataFrame({'icao': icao, 'time': data.index, 'twil': pd.isnull(data.TWIL_y).astype(int)}).reset_index(drop=True))
    pd.concat(twil_data).to_csv('twilight.csv', index=False)

def load_twilight_data(filename):
    twil_df = pd.read_csv(filename, usecols=['icao', 'time', 'twil'])
    icaos = twil_df['icao'].unique()
    twil_data = {}
    for icao in icaos:
        df = pd.DataFrame(twil_df[twil_df['icao'] == icao])
        df['time'] = pd.to_datetime(df['time'])
        twil_data[icao] = df.set_index('time')
    return twil_data

twil_data = load_twilight_data('twilight.csv')

TIME_DAY = 0
TIME_NIGHT = 1
TIME_SUNSET = 2

def get_time_of_day(timestamp: str, icao: str):
    """
    Returns 0 if daytime, 1 if night or sunset within 30 minutes, 2 if sunset between 30 and 60 mins.

    :param str timestamp: UTC time in format '%Y-%m-%d %H:%M:%S'.
    :param str icao: Airfield code.
    :return: TIME_DAY=0, TIME_NIGHT=1, TIME_SUNSET=2
    :rtype: int
    """
    if icao not in twil_data:
        return None
    twil = twil_data[icao]
    dt = datetime.fromisoformat(timestamp + '+00:00')
    if dt.day == 29 and dt.month == 2:
        dt += timedelta(days=1)
    dt = datetime(year=2022, month=dt.month, day=dt.day, hour=dt.hour, minute=dt.minute, second=dt.second)

    try:
        prev_idx = twil.index[twil.index.get_loc(dt, method='ffill')]
        prev_twil = twil.loc[prev_idx, 'twil']
    except KeyError:
        prev_idx = twil.index[-1]
        prev_twil = twil.loc[prev_idx, 'twil']
        prev_idx = datetime(year=2021, month=prev_idx.month, day=prev_idx.day, hour=prev_idx.hour, minute=prev_idx.minute, second=prev_idx.second)

    if prev_twil == 0:
        return TIME_NIGHT

    try:
        next_idx = twil.index[twil.index.get_loc(dt, method='bfill')]
    except KeyError:
        next_idx = twil.index[0]
        next_idx = datetime(year=2023, month=next_idx.month, day=next_idx.day, hour=next_idx.hour, minute=next_idx.minute, second=next_idx.second)

    if dt + timedelta(minutes=30) >= next_idx:
        return TIME_NIGHT
    if dt + timedelta(minutes=60) >= next_idx:
        return TIME_SUNSET
    return TIME_DAY

if __name__ == '__main__':
    #convert_twilight_data('data/yo_paiva_AIP31MAR16_fixed.csv')
    print(twil_data['EFHK'])
    #print(get_time_of_day('2020-03-31 23:59:00', 'EFHK'))
    print(get_time_of_day('2020-01-01 00:01:00', 'EFHK'))
    #print(get_time_of_day('2022-01-19 14:19:00', 'EFHK'))
