import numpy as np
import pandas as pd
import sqlite3

#from twil_tools import TIME_DAY, TIME_SUNSET, TIME_NIGHT

dbfile = 'db/hems-all.sqlite'

sect_df = pd.read_csv('data/FH_sektorit_0-sken_ver5_06APR22.csv', sep=';')
sect_df.set_index('Sector_no', inplace=True, drop=False)
geo_df = pd.read_csv('data/Saahavaintoasemat.csv', sep=',', index_col='ICAO')

vfr_wx = {}
for sector in sect_df.index:
    vfr_wx[sector] = sect_df.loc[sector, 'vfr_wx'].split(';')

def create_sector_views(dbfile, vfr_wx):
    con = sqlite3.connect(dbfile)
    cur = con.cursor()

    cur.execute('DROP VIEW IF EXISTS wx;')
    sql = """
    CREATE VIEW wx AS
    SELECT metar.icao as icao, metar.time as time,
        metar.temp as temp,
        metar.vis as metar_vis, taf.taf_vis as taf_vis, metar.rvr as metar_rvr,
        metar.ceil as metar_ceil, taf.taf_ceil as taf_ceil,
        metar.base as metar_base, taf.taf_base as taf_base,
        metar.time_of_day as time_of_day,
        metar.metar_msg as metar_msg, taf.taf_msg as taf_msg
    FROM metar
    LEFT JOIN taf
    ON metar.icao = taf.icao AND metar.time = taf.time;
    """
    cur.execute(sql)

    for sector, icaos in vfr_wx.items():
        view_name = f'[VFR_{sector}]'
        print(f'Creating view {view_name}')
        sql = f"DROP VIEW IF EXISTS {view_name};\n"
        cur.execute(sql)

        sql = f"CREATE VIEW {view_name} AS\n"
        sql += """
        SELECT time, substr(time, 6, 2) as month, substr(time, 12, 2) as hour,
            COUNT() as count,
            (MIN(time_of_day) != 0) as night,
            MIN(MIN(vfr_3000_metar), MIN(vfr_3000_taf)) as vfr_3000,
            MIN(MIN(vfr_3000_metar or vfr_2000_metar), MIN(vfr_3000_taf or vfr_2000_taf)) as vfr_2000,
            MIN(MIN(vfr_3000_metar or vfr_2000_metar or vfr_500_metar), MIN(vfr_3000_taf or vfr_2000_taf or vfr_500_taf)) as vfr_500,
            MIN(MIN(vfr_night_metar), MIN(vfr_night_taf)) as vfr_night,
            MIN(MIN(vfr_night_few_cloud_metar), MIN(vfr_night_few_cloud_taf)) as vfr_night_few_cloud
        FROM (
        """
        first = True
        for icao in icaos:
            if not first:
                sql += 'UNION\n'
            first = False
            sql += """
            SELECT icao, time,
                metar_vis, taf_vis, metar_ceil, taf_ceil, metar_base, taf_base, time_of_day,
                (metar_vis >= 3000 and metar_ceil >= 300) as vfr_3000_metar,
                (metar_vis >= 2000 and metar_ceil >= 400) as vfr_2000_metar,
                (metar_vis >= 500 and metar_ceil >= 500) as vfr_500_metar,
                (metar_vis >= 3000 and metar_base >= 1200) as vfr_night_metar,
                (metar_vis >= 3000 and metar_ceil >= 1200 and metar_base < 1200) OR (metar_vis >= 3000 AND metar_base >= 1200) as vfr_night_few_cloud_metar,
                (taf_vis >= 3000 and taf_ceil >= 300) as vfr_3000_taf,
                (taf_vis >= 2000 and taf_ceil >= 400) as vfr_2000_taf,
                (taf_vis >= 500 and taf_ceil >= 500) as vfr_500_taf,
                (taf_vis >= 3000 and taf_base >= 1200) as vfr_night_taf,
                (taf_vis >= 3000 and taf_ceil >= 1200 and taf_base < 1200) OR (taf_vis >= 3000 AND taf_base >= 1200) as vfr_night_few_cloud_taf
            FROM wx
            """
            sql += f"WHERE icao = '{icao}' AND metar_vis IS NOT NULL AND metar_ceil IS NOT NULL AND metar_base IS NOT NULL\n"
        sql += f') GROUP BY time HAVING count = {len(icaos)} AND MAX(time_of_day) IS NOT NULL\n'
        sql += """
            AND vfr_3000_taf IS NOT NULL
            AND vfr_2000_taf IS NOT NULL
            AND vfr_500_taf IS NOT NULL
            AND vfr_night_taf IS NOT NULL
            AND vfr_night_few_cloud_taf IS NOT NULL;
        """

        cur.execute(sql)

    con.commit()
    con.close()

def get_combinations(dbfile, vfr_wx):
    con = sqlite3.connect(dbfile)

    combinations = None
    for i, sector in enumerate(vfr_wx.keys()):
        #if not sector.startswith('FH51'):
        #if i < 76:
        #    continue
        view_name = f'[VFR_{sector}]'
        print(f"Processing sector {sector} ({i+1}/{len(vfr_wx)})")
        sql = """SELECT COUNT() as n,
                        vfr_3000, vfr_2000, vfr_500,
                        vfr_night, vfr_night_few_cloud, night
        FROM {}
        GROUP BY vfr_3000, vfr_2000, vfr_500,
                 vfr_night, vfr_night_few_cloud, night;
        """.format(view_name)
        chunk = pd.read_sql(sql, con, coerce_float=False)
        chunk['night'] = chunk['night'].astype(float).astype('Int64')
        if combinations is None:
            combinations = chunk
        else:
            combinations = pd.concat([combinations, chunk])
    con.close()
    cols = ['vfr_3000', 'vfr_2000', 'vfr_500', 'vfr_night', 'vfr_night_few_cloud', 'night']
    combinations.fillna(-999, inplace=True)
    c = pd.DataFrame(combinations.groupby(cols, as_index=False).sum())
    c[cols] = c[cols].applymap(np.int64)
    c.to_csv('output/vfr-combinations.csv', index=False)
    return c

def list_criteria(dbfile, vfr_wx):
    con = sqlite3.connect(dbfile)

    table = None
    for i, sector in enumerate(vfr_wx.keys()):
        view_name = f'[VFR_{sector}]'
        print(f"Processing sector {sector} ({i+1}/{len(vfr_wx)})")
        sql = """
        SELECT month, hour,
               vfr_3000, vfr_2000, vfr_500,
               vfr_night, vfr_night_few_cloud, night,
               COUNT() as n
        FROM {}
        GROUP BY month, hour,
                 vfr_3000, vfr_2000, vfr_500,
                 vfr_night, vfr_night_few_cloud, night
        ORDER BY month, hour
        """.format(view_name)
        chunk = pd.read_sql(sql, con, coerce_float=False)
        chunk['night'] = chunk['night'].astype(float).astype('Int64')
        chunk.insert(0, 'sector', sector)
        if table is None:
            table = chunk
        else:
            table = pd.concat([table, chunk])
    con.close()
    cols = ['vfr_3000', 'vfr_2000', 'vfr_500', 'vfr_night', 'vfr_night_few_cloud', 'night']
    table[cols] = table[cols].applymap(np.int64)
    table.to_csv('output/vfr-nolabels.csv', index=False)
    return table

def assign_labels(vfr_table, classlabels):
    df = vfr_table.merge(classlabels, on=['vfr_3000', 'vfr_2000', 'vfr_500', 'vfr_night', 'vfr_night_few_cloud', 'night'], how='left')
    df = df[['sector', 'month', 'hour', 'n', 'vfr_label', 'vfr_ok']]
    df = df.groupby(['sector', 'month', 'hour', 'vfr_label', 'vfr_ok'], as_index=False).sum()
    vfr_ok = df[['sector', 'month', 'hour', 'vfr_ok', 'n']].groupby(['sector', 'month', 'hour', 'vfr_ok']).sum().reset_index()
    vfr_ok = vfr_ok.pivot(index=['sector', 'month', 'hour'], columns=['vfr_ok'], values='n')
    df = df.pivot(index=['sector', 'month', 'hour'], columns=['vfr_label'], values='n')
    df.fillna(0, inplace=True)
    df.insert(0, 'n', df.sum(axis=1))
    labs = classlabels['vfr_label'].unique()
    ren = {lab: f'n_{lab}' for lab in labs}
    df.rename(columns=ren, inplace=True)
    for lab in labs:
        df[lab] = df[f'n_{lab}'] / df['n']
    df['n_VFR_OK'] = vfr_ok['VFR_OK'].fillna(0)
    df['VFR_OK'] = (df['n_VFR_OK'] / df['n']).fillna(0)
    df.to_csv('output/vfr-labels.csv', index=True)
    with pd.ExcelWriter('output/vfr-labels.xlsx') as writer:
        df = df.reset_index()
        df.to_excel(writer, 'FHXX_all', index=False, freeze_panes=(1, 0))
        for sector in vfr_table['sector'].unique():
            sector_df = df[df['sector'] == sector]
            sector_df.to_excel(writer, sector, index=False, freeze_panes=(1, 0))

def pivot_csv(vfr_df, outfile):
    vfr_df = vfr_df[['sector', 'month', 'hour', 'VFR_OK']].copy()
    vfr_df['month'] = vfr_df['month'].astype(str).str.zfill(2)
    vfr_df['hour'] = vfr_df['hour'].astype(str).str.zfill(2)
    vfr_df['t'] = vfr_df['month'].str.cat(vfr_df['hour'].astype(str), sep='_')
    vfr_df = vfr_df[['sector', 't', 'VFR_OK']]
    df = vfr_df.pivot(index=['sector'], columns=['t'], values='VFR_OK')
    df.to_csv(outfile, index=True)

def create_validation_views(dbfile, vfr_wx, classlabels):
    con = sqlite3.connect(dbfile)

    classlabels.to_sql('vfr_classlabels', con, if_exists='replace', index=False)

    cur = con.cursor()

    for sector, icaos in vfr_wx.items():
        view_name = f'[val_VFR_{sector}]'
        sect_view_name = f'[VFR_{sector}]'
        print(f'Creating view {view_name}')
        sql = f"DROP VIEW IF EXISTS {view_name};\n"
        cur.execute(sql)

        sql = f"CREATE VIEW {view_name} AS\n"
        sql += "SELECT DISTINCT sector.time, lab.vfr_label as vfr_label, sector.night as night,\n"

        first = True
        for icao in icaos:
            if not first:
                sql += ','
            first = False
            sql += f"""
            metar_{icao}.vis as {icao}_vis, metar_{icao}.ceil as {icao}_ceil, metar_{icao}.base as {icao}_base, metar_{icao}.metar_msg as {icao}_metar
            """
        sql += f'FROM {sect_view_name} sector\n'
        for icao in icaos:
            sql += f"""
            INNER JOIN metar metar_{icao}
            ON sector.time = metar_{icao}.time AND metar_{icao}.icao = '{icao}'
            """
        sql += """INNER JOIN vfr_classlabels lab
            ON sector.vfr_3000 = lab.vfr_3000 AND
            sector.vfr_2000 = lab.vfr_2000 AND
            sector.vfr_500 = lab.vfr_500 AND
            sector.vfr_night = lab.vfr_night AND
            sector.vfr_night_few_cloud = lab.vfr_night_few_cloud AND
            sector.night = lab.night
            """
        cur.execute(sql)

    con.commit()
    con.close()

def get_random_sample(dbfile, vfr_wx):
    n_per_sector = 1000

    samples = {}
    con = sqlite3.connect(dbfile)
    for i, sector in enumerate(vfr_wx):
        print(f'Getting {n_per_sector} samples from {sector} ({i+1}/{len(vfr_wx)})')
        view_name = f'[val_VFR_{sector}]'
        sql = f'SELECT * FROM {view_name} ORDER BY RANDOM() LIMIT {n_per_sector}'
        sector_sample = pd.read_sql(sql, con)
        #sector_sample['time_of_day']
        #mask = sector_sample['time_of_day'] == TIME_DAY
        #sector_sample.loc[mask, 'time_of_day'] = 'DAY'
        #mask = sector_sample['time_of_day'] == TIME_SUNSET
        #sector_sample.loc[mask, 'time_of_day'] = 'SUNSET'
        #mask = sector_sample['time_of_day'] == TIME_NIGHT
        #sector_sample.loc[mask, 'time_of_day'] = 'NIGHT'
        samples[sector] = sector_sample
    con.close()

    with pd.ExcelWriter('output/vfr-random-sample.xlsx') as writer:
        for sector, sample in samples.items():
            print(f'Writing {sector} samples to xlsx...')
            sample.to_excel(writer, sector, index=False, freeze_panes=(1, 0))

if __name__ == "__main__":
    import sys
    import argparse
    class Parser(argparse.ArgumentParser):
        def error(self, message):
            sys.stderr.write('error: %s\n' % message)
            self.print_help()
            sys.exit(2)
    parser = Parser(description='VFR classification')
    parser.add_argument('action', type=str, nargs=1, help='createviews | getcombinations | listcriteria | assignlabels | pivotcsv | createvalviews | randomsample')
    #parser.add_argument('--outputdir', type=str, default='plots', help='output directory')
    args = parser.parse_args()

    if args.action[0] not in ['createviews', 'getcombinations', 'listcriteria', 'assignlabels', 'pivotcsv', 'createvalviews', 'randomsample']:
        print('Invalid action')
        exit()

    if args.action[0] == 'createviews':
        create_sector_views(dbfile, vfr_wx)
        exit()

    if args.action[0] == 'getcombinations':
        print(get_combinations(dbfile, vfr_wx))
        exit()

    if args.action[0] == 'listcriteria':
        list_criteria(dbfile, vfr_wx)
        exit()

    if args.action[0] == 'assignlabels':
        vfr_table = pd.read_csv('output/vfr-nolabels.csv')
        classlabels = pd.read_excel('data/hems-classlabels.xlsx', sheet_name='VFR0')
        assign_labels(vfr_table, classlabels)
        exit()

    if args.action[0] == 'pivotcsv':
        vfr_df = pd.read_csv('output/vfr-labels.csv')
        pivot_csv(vfr_df, 'output/vfr-proba-only.csv')
        exit()

    if args.action[0] == 'createvalviews':
        classlabels = pd.read_excel('data/hems-classlabels.xlsx', sheet_name='VFR0')
        create_validation_views(dbfile, vfr_wx, classlabels)
        exit()

    if args.action[0] == 'randomsample':
        get_random_sample(dbfile, vfr_wx)
        exit()
