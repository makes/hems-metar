import numpy as np
import pandas as pd
import sqlite3

dbfile = 'db/hems-all-notafcloud-indexed.sqlite'

sect_df = pd.read_csv('data/FH-base_sektorit_ver3_28MAR22.csv', sep=';')
sect_df.set_index('Sector_no', inplace=True, drop=False)
geo_df = pd.read_csv('data/Saahavaintoasemat.csv', sep=',', index_col='ICAO')

vfr_wx = {}
for sector in sect_df.index:
    vfr_wx[sector] = sect_df.loc[sector, 'vfr_wx'].split(';')

def create_sector_views(dbfile, vfr_wx):
    con = sqlite3.connect(dbfile)
    cur = con.cursor()

    for sector, icaos in vfr_wx.items():
        view_name = f'[VFR_{sector}]'
        print(f'Creating view {view_name}')
        sql = f"DROP VIEW IF EXISTS {view_name};\n"
        cur.execute(sql)

        sql = f"CREATE VIEW {view_name} AS\n"
        sql += """
        SELECT time, substr(time, 6, 2) as month, substr(time, 12, 2) as hour,
               COUNT() as count,
               MAX(time_of_day) as time_of_day,
               MIN(vfr_3000) as vfr_3000,
               MIN(vfr_3000 or vfr_2000) as vfr_2000,
               MIN(vfr_3000 or vfr_2000 or vfr_500) as vfr_500,
               MIN(vfr_night) as vfr_night, MIN(vfr_night_few_cloud) as vfr_night_few_cloud
        FROM (
        """
        first = True
        for icao in icaos:
            if not first:
                sql += 'UNION\n'
            first = False
            sql += """
            SELECT metar.icao AS icao, metar.time as time,
                metar.vis as vis, metar.ceil as ceil, metar.base as base, metar.time_of_day as time_of_day,
                (metar.vis >= 3000 and metar.ceil >= 300) as vfr_3000,
                (metar.vis >= 2000 and metar.ceil >= 400) as vfr_2000,
                (metar.vis >= 500 and metar.ceil >= 500) as vfr_500,
                (metar.vis >= 3000 and metar.base >= 1200) as vfr_night,
                (metar.vis >= 3000 and metar.ceil >= 1200 and metar.base < 1200) as vfr_night_few_cloud,
                metar.time_of_day as time_of_day
            FROM metar
            """
            sql += f"WHERE icao = '{icao}'\n"
        sql += f') GROUP BY time HAVING count = {len(icaos)}\n'
        sql += """
        and time_of_day IS NOT NULL
        and vfr_3000 IS NOT NULL
        and vfr_2000 IS NOT NULL
        and vfr_500 IS NOT NULL
        and vfr_night IS NOT NULL
        and vfr_night_few_cloud IS NOT NULL;
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
                        vfr_night, vfr_night_few_cloud, time_of_day
        FROM {}
        GROUP BY vfr_3000, vfr_2000, vfr_500,
                 vfr_night, vfr_night_few_cloud, time_of_day;
        """.format(view_name)
        chunk = pd.read_sql(sql, con, coerce_float=False)
        chunk['time_of_day'] = chunk['time_of_day'].astype(float).astype(int)
        if combinations is None:
            combinations = chunk
        else:
            combinations = pd.concat([combinations, chunk])
    con.close()
    cols = ['vfr_3000', 'vfr_2000', 'vfr_500', 'vfr_night', 'vfr_night_few_cloud', 'time_of_day']
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
               vfr_night, vfr_night_few_cloud, time_of_day,
               COUNT() as n
        FROM {}
        GROUP BY month, hour,
                 vfr_3000, vfr_2000, vfr_500,
                 vfr_night, vfr_night_few_cloud, time_of_day
        ORDER BY month, hour
        """.format(view_name)
        chunk = pd.read_sql(sql, con, coerce_float=False)
        chunk['time_of_day'] = chunk['time_of_day'].astype(float).astype(int)
        chunk.insert(0, 'sector', sector)
        if table is None:
            table = chunk
        else:
            table = pd.concat([table, chunk])
    con.close()
    cols = ['vfr_3000', 'vfr_2000', 'vfr_500', 'vfr_night', 'vfr_night_few_cloud', 'time_of_day']
    table[cols] = table[cols].applymap(np.int64)
    table.to_csv('output/vfr-nolabels.csv', index=False)
    return table

def assign_labels(vfr_table, classlabels):
    df = vfr_table.merge(classlabels, on=['vfr_3000', 'vfr_2000', 'vfr_500', 'vfr_night', 'vfr_night_few_cloud', 'time_of_day'], how='left')
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

if __name__ == "__main__":
    import sys
    import argparse
    class Parser(argparse.ArgumentParser):
        def error(self, message):
            sys.stderr.write('error: %s\n' % message)
            self.print_help()
            sys.exit(2)
    parser = Parser(description='VFR classification')
    parser.add_argument('action', type=str, nargs=1, help='createviews | getcombinations | listcriteria | assignlabels | pivotcsv')
    #parser.add_argument('--outputdir', type=str, default='plots', help='output directory')
    args = parser.parse_args()

    if args.action[0] not in ['createviews', 'getcombinations', 'listcriteria', 'assignlabels', 'pivotcsv']:
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
