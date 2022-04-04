import os
import time
import random
import logging
import numpy as np
import pandas as pd
import sqlite3

import metar_tools
import taf_tools
from twil_tools import get_time_of_day

data_yrs = list(range(2011, 2021+1))

# CREATE TABLE t_metar (
#     icao TEXT,
#     time TEXT,
#     temp INTEGER,
#     vis INTEGER,
#     rvr INTEGER,
#     ceil INTEGER,
#     base INTEGER,
#     time_of_day INTEGER,
#     metar_msg TEXT
# );

# CREATE TABLE t_taf (
#     icao TEXT,
#     time TEXT,
#     taf_vis INTEGER,
#     taf_msg TEXT
# );

# CREATE TABLE t_hems_class (
#     sect TEXT,
#     time TEXT,
#     icao_alt TEXT,
#     class TEXT
# );

# CREATE INDEX idx_metar ON t_metar (icao, time)
# CREATE INDEX idx_taf ON t_taf (icao, time)
# CREATE INDEX idx_hems_class ON t_hems_class (sect, time)

# (t_metar(term) INNER JOIN t_taf) LEFT JOIN t_metar(sect)

def find_first_file(icao_code, type):
    for i, year in enumerate(data_yrs):
        if os.path.isfile(f'data/{type.lower()}/{icao_code}_{year}_{type}.dat'):
            return i
        logging.info(f'No {type} file for {icao_code} in year {year}.')
    raise RuntimeError("No data found")

def load_metar_chunk(icao_code, year):
    cols = ['ttime', 'content']
    df = pd.read_csv(f'data/metar/{icao_code}_{year}_METAR.dat', sep=';', usecols=cols)
    df['time'] = pd.to_datetime(df['ttime'])
    return df.set_index('time')

def load_metar(icao_code):
    try:
        i = find_first_file(icao_code, 'METAR')
    except RuntimeError as e:
        logging.warn(f"{str(e)} - {icao_code}")
        return None
    metar = load_metar_chunk(icao_code, data_yrs[i])
    for year in data_yrs[i+1:]:
        try:
            df = load_metar_chunk(icao_code, year)
        except:
            logging.info(f'No METAR file for {icao_code} in year {year}.')
        metar = pd.concat((metar, df))
    return metar

def load_taf_chunk(icao_code, year):
    cols = ['ttime', 'content']
    df = pd.read_csv(f'data/taf/{icao_code}_{year}_TAF.dat', sep=';', usecols=cols)
    df['time'] = pd.to_datetime(df['ttime'])
    df.set_index('time', inplace=True)
    return df

def load_taf(icao_code):
    try:
        i = find_first_file(icao_code, 'TAF')
    except RuntimeError as e:
        logging.warn(f"{str(e)} - {icao_code}")
        return None
    taf = load_taf_chunk(icao_code, data_yrs[i])
    for year in data_yrs[i+1:]:
        try:
            df = load_taf_chunk(icao_code, year)
        except:
            logging.info(f'No TAF file for {icao_code} in year {year}.')
        taf = pd.concat((taf, df))

    # handling to drop duplicate time indexes
    #taf['ttime'] = taf.index.strftime('%Y-%m-%d %H:%M:%S')
    taf.drop_duplicates(subset=['ttime'], keep='last', inplace=True)

    taf = taf.asfreq('10min')
    ffill_limit = 200 # don't fill super long gaps. TAF time rolls over in a month,
                      # max validity 30 h so anything between 30 h and 1 month goes.
    taf['ttime'] = taf.index.strftime('%Y-%m-%d %H:%M:%S')
    taf['content'] = taf['content'].ffill(limit=ffill_limit)
    return taf.dropna()

def extract_metar(data: np.ndarray):
    time_str = data[0]
    metar_str = data[1]
    obs = metar_tools.parse(metar_str, year=int(time_str[:4]), month=int(time_str[5:7]))
    temp = metar_tools.get_temp(obs)
    vis = metar_tools.get_vis(obs)
    rvr = metar_tools.get_rvr(obs)
    ceil = metar_tools.get_ceil(obs)
    base = metar_tools.get_base(obs, ceil)
    icao = metar_tools.get_icao(obs)
    time_of_day = get_time_of_day(time_str, icao)
    return (icao, time_str, temp, vis, rvr, ceil, base, time_of_day, metar_str)

def transform_metar(metar):
    metar_cols = ['icao', 'time', 'temp', 'vis', 'rvr', 'ceil', 'base', 'time_of_day', 'metar_msg']
    metar_trans = [extract_metar(d) for d in zip(metar.ttime, metar.content)]
    df = pd.DataFrame(metar_trans, columns=metar_cols)
    df['time_of_day'] = df['time_of_day'].astype(float).astype('Int64') # patch
    return df

def extract_taf(data: np.ndarray):
    time_str = data[0]
    taf_str = data[1]
    try:
        forecast = taf_tools.parse(taf_str)
    except (ValueError, IndexError) as e:
        logging.error(f'{str(e)} - {taf_str}')
        return (None, time_str, None, taf_str)
    icao = forecast.station
    vis = taf_tools.get_worstcase_vis(forecast, time_str, 3)
    return (icao, time_str, vis, taf_str)

def transform_taf(taf):
    taf_cols = ['icao', 'time', 'taf_vis', 'taf_msg']
    taf_trans = [extract_taf(d) for d in zip(taf.ttime, taf.content)]
    df = pd.DataFrame(taf_trans, columns=taf_cols)
    return df

def load_station(icao):
    logging.info(f"Reading {icao} METAR...")
    metar = load_metar(icao)
    if metar is not None:
        logging.info(f"Transforming {icao} METAR...")
        metar = transform_metar(metar)
    logging.info(f"Reading {icao} TAF...")
    taf = load_taf(icao)
    if taf is not None:
        logging.info(f"Transforming {icao} TAF...")
        taf = transform_taf(taf)
    return metar, taf

def chunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in range(0, len(l), n))

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    task_id = os.getenv("SLURM_ARRAY_TASK_ID")
    task_id = int(task_id) if task_id is not None else None

    sect_df = pd.read_csv('data/FH-base_sektorit.csv', sep=',', index_col='Sector_no')
    geo_df = pd.read_csv('data/Saahavaintoasemat.csv', sep=',', index_col='ICAO')

    DBNAME = "db/hems-all.sqlite"
    # all:
    icaos = list(geo_df.index)
    random.Random(7).shuffle(icaos)
    # FH10:
    #icaos = ['EFHK', 'EFTP', 'EFTU', 'ILZS', 'ILIK', 'ILZL', 'ILZZ', 'ILZV', 'ILZW']
    # FH50:
    #icaos = ['EFOU', 'EFRO', 'EFKU', 'ILYL', 'ILQP', 'ILQH', 'ILQJ', 'ILRU', 'ILPU', 'ILQY', 'ILXW']

    if task_id is not None:
        icaos = list(chunks(icaos, 10))[task_id]

    if task_id is None or task_id == 0:
        if os.path.exists(DBNAME):
            os.remove(DBNAME)
    elif task_id is not None and task_id > 0:
        while not os.path.exists(DBNAME):
            time.sleep(1)

    con = sqlite3.connect(DBNAME)

#    if task_id is None or task_id == 0:
#        with con:
#            con.execute('PRAGMA journal_mode=WAL')

    for icao in icaos:
        logging.info(f"Processing {icao}")
        metar, taf = load_station(icao)
        if metar is not None:
            logging.info(f"Inserting {icao} METAR to DB...")
            while True:
                try:
                    with con:
                        metar.to_sql('metar', con, if_exists='append', index=False)
                    break
                except sqlite3.OperationalError as e:
                    logging.error(f"{str(e)} - {icao} METAR")
                    time.sleep(2)
        if taf is not None:
            logging.info(f"Inserting {icao} TAF to DB...")
            while True:
                try:
                    with con:
                        taf.to_sql('taf', con, if_exists='append', index=False)
                    break
                except sqlite3.OperationalError as e:
                    logging.error(f"{str(e)} - {icao} TAF")
                    time.sleep(2)

# CREATE INDEX icao_metar ON metar (icao);
# CREATE INDEX icao_time_metar ON metar (icao, time);
#
# CREATE INDEX icao_taf ON taf (icao);
# CREATE INDEX icao_time_taf ON taf (icao, time);

    con.close()