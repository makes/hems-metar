import os
import pandas as pd
import sqlite3
#import matplotlib.pyplot as plt
from classify_hems import check_vfr
from classify_hems import check_ifr

import classify

dbname = "db/hems-all-notafcloud.sqlite"

sect_df = pd.read_csv('data/FH-base_sektorit.csv', sep=',')
sect_df.set_index('Sector_no', inplace=True, drop=False)
geo_df = pd.read_csv('data/Saahavaintoasemat.csv', sep=',', index_col='ICAO')

con = sqlite3.connect(dbname)

sect_df[['Sector_no', 'dep_dest', 'sect_wx', 'cld_ceiling', 'alt_open', 'alt_all']].to_sql('sect', con, if_exists='replace', index=False)

vfr_allowing_classes = ['DAY HEMS VFR', 'SUNDOWN HEMS VFR', 'NIGHT HEMS VFR']
ifr_allowing_classes = ['DAY HEMS IFR', 'DAY HEMS VFR RETURN NIGHT IFR', 'NIGHT HEMS IFR']
icing_classes = ['DAY IFR ICING', 'NIGHT IFR ICING']


output_path=f'output/vfr-all.csv'
if os.path.exists(output_path):
    open(output_path, 'w').close()  # clear file

sect_names = sect_df.index

for sect_name in sect_names:
    print(f"reading sector {sect_name}")  # (< 1 min)
    df = classify.read_sector_df(con, sect_name)
    if df.empty:
        print(f"❌ WARNING: No data for sector {sect_name}. Skipping.")
        continue

    icao_term = sect_df.loc[sect_name, 'dep_dest']
    alt_open = sect_df.loc[sect_name, 'alt_open'].split(';')
    alt_all = sect_df.loc[sect_name, 'alt_all'].split(';')
    cld_ceiling = sect_df.loc[sect_name, 'cld_ceiling']

    #print(f"reading alts for {sect_name}")  # (< 10 s)
    #alts = alt_open
    #alt_df = classify.read_alt_df(con, alts)
    df.dropna(inplace=True)  # TODO don't drop all na?

    print(f"classifying VFR for {sect_name}")  # (< 10 s)
    df['vfr_class'] = df.apply(check_vfr, axis=1)
    #print(f"classifying IFR for {sect_name}")  # (> 6 mins)
    #df['ifr_class'] = df.apply(lambda row: check_ifr(row, alt_df, alts, cld_ceiling), axis=1)

    print("creating month an hour columns")
    df['month'] = df.time.str.slice(5, 7).astype(int)
    df['hour'] = df.time.str.slice(11, 13).astype(int)

    print("creating VFR column")
    df['VFR'] = df.vfr_class.isin(vfr_allowing_classes).astype(int)
    #print("creating IFR column")
    #df['IFR'] = df.ifr_class.isin(ifr_allowing_classes).astype(int) & (df.VFR == 0)
    #print("creating ICING column")
    #df['NO FLY ICING'] = df.ifr_class.isin(icing_classes).astype(int) & (df.VFR == 0) & (df.IFR == 0)
    #print("creating NO FLY column")
    #df['NO FLY'] = (df.VFR == 0) & (df.IFR == 0) & (df['NO FLY ICING'] == 0)

    print(f"building hourly summary for {sect_name}")
    #hourly = classify.make_hourly_csv(df, sect_name)
    hourly = classify.make_hourly_csv_vfr_only(df, sect_name)

    hourly.to_csv(output_path, mode='a', header=not os.path.exists(output_path))
    #hourly.to_csv(f'output/canfly-{sect_name}.csv')

con.close()