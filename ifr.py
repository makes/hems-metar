import numpy as np
import pandas as pd
import sqlite3

DBFILE = 'db/hems-all.sqlite'

def create_alt_views(dbfile, ifr_alt_params):
    con = sqlite3.connect(dbfile)
    cur = con.cursor()

    for sector, params in ifr_alt_params.items():
        icaos, taf_val = params
        view_name = f'[ALT_{sector}]'
        print(f'Creating view {view_name}')
        sql = f"DROP VIEW IF EXISTS {view_name};\n"
        cur.execute(sql)

        sql = f"CREATE VIEW {view_name} AS\n"
        sql += """
        SELECT time, substr(time, 6, 2) as month, substr(time, 12, 2) as hour,
            COUNT() as count,
            (MIN(time_of_day) != 0) as night,
            MAX(alt_min) as alt_min,
            MAX(alt_t7) as t7,
            MAX(alt_min7) as alt_min7,
            MAX(alt_t2) as t2,
            MAX(alt_min2) as alt_min2
        FROM (
        """
        first = True
        for icao in icaos:
            if not first:
                sql += 'UNION\n'
            first = False
            sql += f"""
            SELECT icao, time,
                temp, metar_rvr, taf{taf_val}h_vis as taf_vis, metar_ceil, taf{taf_val}h_ceil as taf_ceil, time_of_day,
                (metar_rvr >= 900 and metar_ceil >= 400) and (taf{taf_val}h_vis * (case when (time_of_day != 0) then 2 else 1.5 end) >= 900 and taf{taf_val}h_ceil >= 400) as alt_min,
                (temp >= 7) as alt_t7,
                (temp >= 7) AND (metar_rvr >= 900 and metar_ceil >= 400) and (taf{taf_val}h_vis * (case when (time_of_day != 0) then 2 else 1.5 end) >= 900 and taf{taf_val}h_ceil >= 400) as alt_min7,
                (temp >= 2) as alt_t2,
                (temp >= 2) AND (metar_rvr >= 900 and metar_ceil >= 400) and (taf{taf_val}h_vis * (case when (time_of_day != 0) then 2 else 1.5 end) >= 900 and taf{taf_val}h_ceil >= 400) as alt_min2
            FROM wx
            """
            sql += f"WHERE icao = '{icao}'\n"
        sql += f') GROUP BY time HAVING count = {len(icaos)} AND night IS NOT NULL'
        sql += """
            AND alt_min IS NOT NULL
            AND t7 IS NOT NULL
            AND alt_min7 IS NOT NULL
            AND t2 IS NOT NULL
            AND alt_min2 IS NOT NULL;
        """

        cur.execute(sql)

    con.commit()
    con.close()

def create_dummy_ifr_view(dbfile, ifr_params):
    con = sqlite3.connect(dbfile)
    cur = con.cursor()
    p = ifr_params
    view_name = f"[IFR_{p['sector']}]"

    print(f'Creating view {view_name}')
    sql = f"DROP VIEW IF EXISTS {view_name};\n"
    cur.execute(sql)

    sql = f"CREATE VIEW {view_name} AS\n"
    sql += f"""
    SELECT vfr.time as time, substr(vfr.time, 6, 2) as month, substr(vfr.time, 12, 2) as hour,
        vfr.vfr_label as vfr_label,
        vfr.night as night,
        0 as ifr_limit,
        0 as alternate,
        0 as t7,
        0 as alternate7,
        0 as t2,
        0 as alternate2,
        0 as cld_break,
        0 as cld_break_apch,
        0 as onscene_vfr,
        0 as apch_min
    FROM [VFR_{sector}] vfr
    """
    cur.execute(sql)

    con.commit()
    con.close()


def create_ifr_view(dbfile, ifr_params, use_taf=True):
    con = sqlite3.connect(dbfile)
    cur = con.cursor()

    if not ifr_params['ifr_bool']:
        create_dummy_ifr_view(dbfile, ifr_params)
        return

    p = ifr_params
    view_name = f"[IFR_{p['sector']}]"
    vfr_view = f"VFR_{p['sector']}"

    print(f'Creating view {view_name}')

    #ifr_limit_crit = f"(dest_ifr.metar_rvr >= 500) AND (dest_ifr.taf{p['taf_val']}h_vis * (case when (vfr.night != 0) then 2 else 1.5 end) >= 500)"
    ifr_limit_crit = f"""
    (dest_ifr.metar_rvr >= 500 and dep_ifr.metar_rvr >= 500) and
    (dest_ifr.taf{p['taf_val']}h_vis * (case when (vfr.night == 1) then 2 else 1.5 end) >= 500) and
    IFNULL(dep_ifr.taf{p['taf_val']}h_vis * (case when (vfr.night == 1) then 2 else 1.5 end) >= 500, 1)"""
    if not use_taf:
        vfr_view = f"VFR_notaf_{p['sector']}"
        ifr_limit_crit = f"(dest_ifr.metar_rvr >= 500 and dep_ifr.metar_rvr >= 500)"

    apch_crit = '0'
    apch_val_cols = ''
    if p['apch_bool'] == 1:
        apch_crit = f"(apch.metar_vis >= {p['sect_apch_wx_vis_m']} and apch.metar_ceil >= {p['sect_apch_wx_ceiling_ft']})"
        apch_val_cols = f"""-- apch
        apch.metar_vis as metar_vis_apch, {p['sect_apch_wx_vis_m']} as apch_min_vis, apch.metar_ceil as metar_ceil_apch, {p['sect_apch_wx_ceiling_ft']} as apch_min_ceil,
        apch.metar_msg as metar_apch,
        """

    #else:
    #    p['sect_apch_wx_vis_m'] = 'null'
    #    p['sect_apch_wx_ceiling_ft'] = 'null'

    sql = f"DROP VIEW IF EXISTS {view_name};\n"
    cur.execute(sql)

    sql = f"CREATE VIEW {view_name} AS\n"
    sql += f"""
    SELECT dest_ifr.time as time, substr(dest_ifr.time, 6, 2) as month, substr(dest_ifr.time, 12, 2) as hour,
        vfr.vfr_label as vfr_label, vfr.night as night,

        -- for validation:
        {p['taf_val']} as taf_valid_h_dest,

        -- dest
        dest_ifr.temp as temp_dest, dest_ifr.metar_rvr as metar_rvr_dest, dest_ifr.metar_ceil as metar_ceil_dest,
        dest_ifr.metar_msg as metar_dest,

        dest_ifr.taf{p['taf_val']}h_vis * (case when (vfr.night == 1) then 2 else 1.5 end) as taf_rvr_dest,
        dest_ifr.taf{p['taf_val']}h_ceil as taf_ceil_dest,
        dest_ifr.taf_msg as taf_dest,

        -- dep
        dep_ifr.temp as temp_dep, dep_ifr.metar_rvr as metar_rvr_dep, dep_ifr.metar_ceil as metar_ceil_dep,
        dep_ifr.metar_msg as metar_dep,

        dep_ifr.taf{p['taf_val']}h_vis * (case when (vfr.night == 1) then 2 else 1.5 end) as taf_rvr_dep,
        dep_ifr.taf{p['taf_val']}h_ceil as taf_ceil_dep,
        dep_ifr.taf_msg as taf_dep,

        -- sect
        sect_wx.metar_vis as metar_vis_sect, sect_wx.metar_ceil as metar_ceil_sect, sect_wx.metar_base as metar_base_sect,
        {p['cld_ceiling']} as cld_break_min, {p['cloud_break_min_ft']} as cld_break_apch_min,

        onscene.vfr_label as onscene_vfr_class,
        sect_wx.metar_msg as metar_sect,

        {apch_val_cols}

        -- criteria:
        {ifr_limit_crit} as ifr_limit,
        alt.alt_min as alternate,
        ((dest_ifr.temp >= 7 and dep_ifr.temp >= 7) AND alt.t7) as t7,
        alt.alt_min7 as alternate7,
        ((dest_ifr.temp >= 2 and dep_ifr.temp >= 2) AND alt.t2) as t2,
        alt.alt_min2 as alternate2,
        (sect_wx.metar_ceil >= {p['cld_ceiling']}) and (sect_wx.metar_vis >= 3000) as cld_break,
        (sect_wx.metar_ceil >= {p['cloud_break_min_ft']}) as cld_break_apch,
        (onscene.vfr_label = 'DAY_HEMS_VFR' OR onscene.vfr_label = 'NIGHT_HEMS_VFR') AS onscene_vfr,
        {apch_crit} as apch_min
    FROM wx dest_ifr
    INNER JOIN [ALT_{p['sector']}] alt
    ON dest_ifr.time = alt.time AND dest_ifr.icao = '{p['dest_ifr']}'
    INNER JOIN wx dep_ifr
    ON dep_ifr.time = dest_ifr.time AND dep_ifr.icao = '{p['dep_ifr']}'
    LEFT JOIN wx sect_wx
    ON dest_ifr.time = sect_wx.time AND sect_wx.icao = '{p['sect_wx']}'
    """
    if p['apch_bool'] == 1:
        sql += f"""
        LEFT JOIN wx apch
        ON dest_ifr.time = apch.time AND apch.icao = '{p['sect_apch_wx']}'
        """
    sql += f"""
    LEFT JOIN onscene_vfr onscene
    ON dest_ifr.time = onscene.time AND onscene.sector = '{p['sector']}'
    INNER JOIN [{vfr_view}] vfr
    ON dest_ifr.time = vfr.time
    WHERE ifr_limit IS NOT NULL AND
          dest_ifr.temp IS NOT NULL AND dep_ifr.temp IS NOT NULL AND alt.t7 IS NOT NULL AND
          sect_wx.metar_vis IS NOT NULL AND sect_wx.metar_ceil IS NOT NULL AND
          cld_break_apch IS NOT NULL AND
          onscene.vfr_label IS NOT NULL
    """
    if p['apch_bool'] == 1:
        sql += "    AND apch.metar_vis IS NOT NULL AND apch.metar_ceil IS NOT NULL"

    cur.execute(sql)

    con.commit()
    con.close()

def get_ifr0_combinations(dbfile, sectors):
    con = sqlite3.connect(dbfile)

    combinations = None
    for i, sector in enumerate(sectors):
        view_name = f'[IFR_{sector}]'
        print(f"Processing sector {sector} ({i+1}/{len(sectors)})")
        sql = """
        SELECT vfr_label,
            ifr_limit, t7, alternate7,
            cld_break,
            night, count() as n
        FROM {}
        GROUP BY vfr_label, ifr_limit, t7, alternate7,
                cld_break, night
        ORDER BY vfr_label, n DESC;
        """.format(view_name)
        chunk = pd.read_sql(sql, con, coerce_float=False)
        chunk['night'] = chunk['night'].astype(float).astype('Int64')
        print(chunk.to_string())
        if combinations is None:
            combinations = chunk
        else:
            combinations = pd.concat([combinations, chunk])
    con.close()
    cols = ['vfr_label', 'ifr_limit', 't7', 'alternate7', 'cld_break', 'night']
    combinations.fillna(-999, inplace=True)
    c = pd.DataFrame(combinations.groupby(cols, as_index=False).sum())
    c[cols[1:]] = c[cols[1:]].applymap(np.int64)
    c.to_csv('output/ifr0-combinations.csv', index=False)
    return c

def get_ifr3_combinations(dbfile,
                          sectors,
                          icing_temp=7,
                          outfile='output/ifr3-combinations.csv'):
    con = sqlite3.connect(dbfile)

    combinations = None
    for i, sector in enumerate(sectors):
        view_name = f'[IFR_{sector}]'
        print(f"Processing sector {sector} ({i+1}/{len(sectors)})")
        alt = f'alternate'
        cols = ['vfr_label', 'ifr_limit', 'alternate', 'cld_break_apch', 'onscene_vfr', 'apch_min', 'night']
        if icing_temp is not None:
            assert icing_temp in {7, 2}
            alt = f't{icing_temp}, alternate{icing_temp}'
            cols = ['vfr_label', 'ifr_limit', f't{icing_temp}', f'alternate{icing_temp}', 'cld_break_apch', 'onscene_vfr', 'apch_min', 'night']
        sql = f"""
        SELECT vfr_label,
            ifr_limit,
            {alt},
            cld_break_apch, onscene_vfr, apch_min,
            night, count() as n
        FROM {view_name}
        GROUP BY vfr_label, ifr_limit,
                {alt},
                cld_break_apch, onscene_vfr, apch_min, night
        ORDER BY vfr_label, n DESC;
        """
        chunk = pd.read_sql(sql, con, coerce_float=False)
        chunk['night'] = chunk['night'].astype(float).astype('Int64')
        print(chunk.to_string())
        if combinations is None:
            combinations = chunk
        else:
            combinations = pd.concat([combinations, chunk])
    con.close()
    combinations.fillna(-999, inplace=True)
    c = pd.DataFrame(combinations.groupby(cols, as_index=False).sum())
    c[cols[1:]] = c[cols[1:]].applymap(np.int64)
    c.to_csv(outfile, index=False)
    return c

def generate_scenario_ifr0(dbfile, sectors):
    con = sqlite3.connect(dbfile)

    df = None
    for i, sector in enumerate(sectors):
        print(f"Processing sector {sector} ({i+1}/{len(sectors)})")
        sql = f"""
        SELECT '{sector}' as sector, month, hour, sum(cnt) as n,
            ifnull(sum(case when label = 'DAY_HEMS_VFR' then cnt end), 0) DAY_HEMS_VFR,
            ifnull(sum(case when label = 'DAY_VFR500' then cnt end), 0) DAY_VFR500,
            ifnull(sum(case when label = 'NIGHT_HEMS_VFR' then cnt end), 0) NIGHT_HEMS_VFR,
            ifnull(sum(case when label = 'NIGHT_VFR_HEMS_FEW_CLOUD' then cnt end), 0) NIGHT_VFR_HEMS_FEW_CLOUD,
            ifnull(sum(case when label = 'DAY_HEMS_IFR' then cnt end), 0) DAY_HEMS_IFR,
            ifnull(sum(case when label = 'NIGHT_HEMS_IFR' then cnt end), 0) NIGHT_HEMS_IFR,
            ifnull(sum(case when label = 'DAY_BELOW_IFR' then cnt end), 0) DAY_BELOW_IFR,
            ifnull(sum(case when label = 'DAY_IFR_ICING' then cnt end), 0) DAY_IFR_ICING,
            ifnull(sum(case when label = 'DAY_IFR_NO_ALTERNATE' then cnt end), 0) DAY_IFR_NO_ALTERNATE,
            ifnull(sum(case when label = 'DAY_IFR_NO_CLOUD_BREAK' then cnt end), 0) DAY_IFR_NO_CLOUD_BREAK,
            ifnull(sum(case when label = 'NIGHT_BELOW_IFR' then cnt end), 0) NIGHT_BELOW_IFR,
            ifnull(sum(case when label = 'NIGHT_IFR_ICING' then cnt end), 0) NIGHT_IFR_ICING,
            ifnull(sum(case when label = 'NIGHT_IFR_NO_ALTERNATE' then cnt end), 0) NIGHT_IFR_NO_ALTERNATE,
            ifnull(sum(case when label = 'NIGHT_IFR_NO_CLOUD_BREAK' then cnt end), 0) NIGHT_IFR_NO_CLOUD_BREAK,
            sum(hems_ok) as n_HEMS_OK
        FROM (
        SELECT month, hour, count() as cnt, label, sum(lab.hems_ok) as hems_ok FROM [IFR_{sector}] crit
        LEFT JOIN lab_ifr0 lab
        ON crit.vfr_label = lab.vfr_label
        AND crit.ifr_limit = lab.ifr_limit
        AND crit.t7 = lab.t7
        AND crit.alternate7 = lab.alternate7
        AND crit.cld_break = lab.cld_break
        AND crit.night = lab.night
        GROUP BY month, hour, label
        )
        GROUP BY month, hour;
        """
        chunk = pd.read_sql(sql, con, coerce_float=False)
        if df is None:
            df = chunk
        else:
            df = pd.concat([df, chunk])
    con.close()
    with pd.ExcelWriter('output/HEMS_SCN_IFR0.xlsx') as writer:
        df = df.reset_index(drop=True)
        df['HEMS_OK'] = df['n_HEMS_OK'] / df['n']
        df.to_excel(writer, 'FHXX_all', index=False, freeze_panes=(1, 0))
        for sector in sectors:
            sector_df = df[df['sector'] == sector]
            sector_df.to_excel(writer, sector, index=False, freeze_panes=(1, 0))
    return df


def generate_scenario_ifr3(dbfile,
                           sectors,
                           icing_temp=7,
                           sub_id = None,
                           outfile='output/HEMS_SCN_IFR3.xlsx'):
    con = sqlite3.connect(dbfile)

    tbl_labels = 'lab_ifr3'
    if sub_id is not None:
        tbl_labels = f'lab_ifr3{sub_id}'

    alt_t = f"""
        AND crit.alternate = lab.alternate
        """
    if icing_temp is not None:
        alt_t = f"""
            AND crit.t{icing_temp} = lab.t{icing_temp}
            AND crit.alternate{icing_temp} = lab.alternate{icing_temp}
            """

    df = None
    for i, sector in enumerate(sectors):
        print(f"Processing sector {sector} ({i+1}/{len(sectors)})")
        sql = f"""
        SELECT '{sector}' as sector, month, hour, sum(cnt) as n,
            ifnull(sum(case when label = 'DAY_HEMS_VFR' then cnt end), 0) DAY_HEMS_VFR,
            ifnull(sum(case when label = 'DAY_VFR500' then cnt end), 0) DAY_VFR500,
            ifnull(sum(case when label = 'NIGHT_HEMS_VFR' then cnt end), 0) NIGHT_HEMS_VFR,
            ifnull(sum(case when label = 'DAY_HEMS_IFR_APCH' then cnt end), 0) DAY_HEMS_IFR_APCH,
            ifnull(sum(case when label = 'DAY_HEMS_IFR_CLOUD_BREAK' then cnt end), 0) DAY_HEMS_IFR_CLOUD_BREAK,
            ifnull(sum(case when label = 'NIGHT_HEMS_IFR_CLOUD_BREAK' then cnt end), 0) NIGHT_HEMS_IFR_CLOUD_BREAK,
            ifnull(sum(case when label = 'NIGHT_HEMS_IFR_APCH' then cnt end), 0) NIGHT_HEMS_IFR_APCH,
            ifnull(sum(case when label = 'DAY_BELOW_IFR' then cnt end), 0) DAY_BELOW_IFR,
            ifnull(sum(case when label = 'DAY_IFR_ICING' then cnt end), 0) DAY_IFR_ICING,
            ifnull(sum(case when label = 'DAY_IFR_NO_ALTERNATE' then cnt end), 0) DAY_IFR_NO_ALTERNATE,
            ifnull(sum(case when label = 'DAY_IFR_NO_CLOUD_BREAK' then cnt end), 0) DAY_IFR_NO_CLOUD_BREAK,
            ifnull(sum(case when label = 'NIGHT_BELOW_IFR' then cnt end), 0) NIGHT_BELOW_IFR,
            ifnull(sum(case when label = 'NIGHT_IFR_ICING' then cnt end), 0) NIGHT_IFR_ICING,
            ifnull(sum(case when label = 'NIGHT_IFR_NO_ALTERNATE' then cnt end), 0) NIGHT_IFR_NO_ALTERNATE,
            ifnull(sum(case when label = 'NIGHT_IFR_NO_CLOUD_BREAK' then cnt end), 0) NIGHT_IFR_NO_CLOUD_BREAK,
            sum(hems_ok) as n_HEMS_OK
        FROM (
        SELECT month, hour, count() as cnt, label, sum(lab.hems_ok) as hems_ok FROM [IFR_{sector}] crit
        LEFT JOIN {tbl_labels} lab
        ON crit.vfr_label = lab.vfr_label
        AND crit.ifr_limit = lab.ifr_limit
        {alt_t}
        AND crit.cld_break_apch = lab.cld_break_apch
        AND crit.onscene_vfr = lab.onscene_vfr
        AND crit.apch_min = lab.apch_min
        AND crit.night = lab.night
        GROUP BY month, hour, label
        )
        GROUP BY month, hour;
        """
        chunk = pd.read_sql(sql, con, coerce_float=False)
        if df is None:
            df = chunk
        else:
            df = pd.concat([df, chunk])
    con.close()
    with pd.ExcelWriter(outfile) as writer:
        df = df.reset_index(drop=True)
        df['HEMS_OK'] = df['n_HEMS_OK'] / df['n']
        df.to_excel(writer, 'FHXX_all', index=False, freeze_panes=(1, 0))
        for sector in sectors:
            sector_df = df[df['sector'] == sector]
            sector_df.to_excel(writer, sector, index=False, freeze_panes=(1, 0))
    return df


def validate_ifr0(dbfile, sector, alts, apch_bool):
    con = sqlite3.connect(dbfile)

    apch_vars = "null as metar_vis_apch, null as apch_min_vis, null as metar_ceil_apch, null as apch_min_ceil, null as metar_apch"
    if apch_bool:
        apch_vars = "metar_vis_apch, apch_min_vis, metar_ceil_apch, apch_min_ceil, metar_apch"
    alt1_vars = "null as metar_alt1, null as taf_alt1"
    alt2_vars = "null as metar_alt2, null as taf_alt2"
    alt1_join = ""
    alt2_join = ""
    if len(alts) > 1:
        alt1_vars = "alt1.metar_msg as metar_alt1, alt1.taf_msg as taf_alt1"
        alt1_join = f"LEFT JOIN wx alt1 ON alt1.time = crit.time AND alt1.icao = '{alts[0]}'"
    if len(alts) == 2:
        alt2_vars = "alt2.metar_msg as metar_alt2, alt2.taf_msg as taf_alt2"
        alt2_join = f"LEFT JOIN wx alt2 ON alt2.time = crit.time AND alt2.icao = '{alts[1]}'"
    sql = f"""
    select crit.time as time, crit.vfr_label as vfr_class, lab.label as class,
    taf_valid_h_dest,

    temp_dest, metar_rvr_dest, metar_ceil_dest, metar_dest,
    taf_rvr_dest, taf_ceil_dest, taf_dest,

    temp_dep, metar_rvr_dep, metar_ceil_dep, metar_dep,
    taf_rvr_dep, taf_ceil_dep, taf_dep,

    onscene_vfr_class, metar_vis_sect, metar_ceil_sect, cld_break_min, cld_break_apch_min, metar_base_sect, metar_sect,

    {apch_vars},

    {alt1_vars},
    {alt2_vars}

    from [IFR_{sector}] crit
    {alt1_join}
    {alt2_join}
    LEFT JOIN lab_ifr0 lab
    ON crit.vfr_label = lab.vfr_label
        AND crit.ifr_limit = lab.ifr_limit
        AND crit.t7 = lab.t7
        AND crit.alternate7 = lab.alternate7
        AND crit.cld_break = lab.cld_break
        AND crit.night = lab.night
    ORDER BY RANDOM() LIMIT 500;
    """
    ret = pd.read_sql(sql, con, coerce_float=False)
    con.close()
    return ret

def validate_ifr3(dbfile, sector, alts, apch_bool):
    con = sqlite3.connect(dbfile)

    apch_vars = "null as metar_vis_apch, null as apch_min_vis, null as metar_ceil_apch, null as apch_min_ceil, null as metar_apch"
    if apch_bool:
        apch_vars = "metar_vis_apch, apch_min_vis, metar_ceil_apch, apch_min_ceil, metar_apch"
    alt1_vars = "null as metar_alt1, null as taf_alt1"
    alt2_vars = "null as metar_alt2, null as taf_alt2"
    alt1_join = ""
    alt2_join = ""
    if len(alts) > 1:
        alt1_vars = "alt1.metar_msg as metar_alt1, alt1.taf_msg as taf_alt1"
        alt1_join = f"LEFT JOIN wx alt1 ON alt1.time = crit.time AND alt1.icao = '{alts[0]}'"
    if len(alts) == 2:
        alt2_vars = "alt2.metar_msg as metar_alt2, alt2.taf_msg as taf_alt2"
        alt2_join = f"LEFT JOIN wx alt2 ON alt2.time = crit.time AND alt2.icao = '{alts[1]}'"
    sql = f"""
    select crit.time as time, crit.vfr_label as vfr_class, lab.label as class,

    taf_valid_h_dest,

    temp_dest, metar_rvr_dest, metar_ceil_dest, metar_dest,
    taf_rvr_dest, taf_ceil_dest, taf_dest,

    temp_dep, metar_rvr_dep, metar_ceil_dep, metar_dep,
    taf_rvr_dep, taf_ceil_dep, taf_dep,

    onscene_vfr_class, metar_vis_sect, metar_ceil_sect, cld_break_min, cld_break_apch_min, metar_base_sect, metar_sect,

    {apch_vars},

    {alt1_vars},
    {alt2_vars}

    from [IFR_{sector}] crit
    {alt1_join}
    {alt2_join}
    LEFT JOIN lab_ifr3 lab
            ON crit.vfr_label = lab.vfr_label
            AND crit.ifr_limit = lab.ifr_limit
            AND crit.t7 = lab.t7
            AND crit.alternate7 = lab.alternate7
            AND crit.cld_break_apch = lab.cld_break_apch
            AND crit.onscene_vfr = lab.onscene_vfr
            AND crit.apch_min = lab.apch_min
            AND crit.night = lab.night
    ORDER BY RANDOM() LIMIT 500;
    """
    ret = pd.read_sql(sql, con, coerce_float=False)
    con.close()
    return ret

if __name__ == "__main__":
    import sys
    import argparse
    class Parser(argparse.ArgumentParser):
        def error(self, message):
            sys.stderr.write('error: %s\n' % message)
            self.print_help()
            sys.exit(2)
    parser = Parser(description='IFR classification')
    parser.add_argument('action', type=str, nargs=1, help='createviews | getcombinations0 | getcombinations3[.N] | generate_ifr0 | generate_ifr3[.N] | validate_ifr0 | validate_ifr3')
    args = parser.parse_args()

    if args.action[0] not in ['createviews',
                              'getcombinations0',
                              'getcombinations3',
                              'getcombinations3.1',
                              'getcombinations3.2',
                              'generate_ifr0',
                              'generate_ifr3',
                              'generate_ifr3.1',
                              'generate_ifr3.2',
                              'validate_ifr0',
                              'validate_ifr3']:
        print('Invalid action')
        exit()

    sect_df = pd.read_csv('data/FH_sektorit_3-sken_ver2.1_ceil_cloud_break_05MAY22.csv', sep=',')
    sect_df.set_index('Sector_no', inplace=True, drop=False)

    if args.action[0] == 'createviews':
        ifr_alts = {}
        for sector in sect_df.index:
            ifr_bool = sect_df.loc[sector, 'ifr_boolean']
            if not ifr_bool:
                continue
            alts = sect_df.loc[sector, 'alt_ifr']
            taf_validity = sect_df.loc[sector, 'taf_ifr_alt_h']
            taf_validity = int(taf_validity)
            ifr_alts[sector] = (alts.split(';'), taf_validity)

        create_alt_views(DBFILE, ifr_alts)

        for sector in sect_df.index:
            ifr_params = {}
            ifr_params['ifr_bool'] = sect_df.loc[sector, 'ifr_boolean']
            ifr_params['sector'] = sector
            if ifr_params['ifr_bool']:
                ifr_params['apch_bool'] = int(sect_df.loc[sector, 'sect_apch_boolean'])
                if ifr_params['apch_bool'] == 1:
                    ifr_params['sect_apch_wx'] = sect_df.loc[sector, 'sect_apch_wx']
                    ifr_params['sect_apch_wx_vis_m'] = int(sect_df.loc[sector, 'sect_apch_wx_vis_m'])
                    ifr_params['sect_apch_wx_ceiling_ft'] = int(sect_df.loc[sector, 'sect_apch_wx_ceiling_ft'])
                ifr_params['taf_val'] = int(sect_df.loc[sector, 'taf_ifr_dest_h'])
                ifr_params['cld_ceiling'] = int(sect_df.loc[sector, 'cld_ceiling'])
                ifr_params['cloud_break_min_ft'] = int(sect_df.loc[sector, 'cloud_break_min_ft'])
                ifr_params['dep_ifr'] = sect_df.loc[sector, 'dep_ifr']
                ifr_params['dest_ifr'] = sect_df.loc[sector, 'dest_ifr']
                ifr_params['sect_wx'] = sect_df.loc[sector, 'sect_wx']
            #print(ifr_params, '\n')
            use_taf = True
            if sector.startswith('FH40') or sector.startswith('FH80'):
                use_taf = False
            #if sector in ['FH50_3.2', 'FH60_2.1', 'FH60_2.3', 'FH60_2.4', 'FH51_4.8', 'FH60_3.1', 'FH60_2.8']:
            #    use_taf = False
            create_ifr_view(DBFILE, ifr_params, use_taf)
        exit()

    if args.action[0] == 'getcombinations0':
        print(get_ifr0_combinations(DBFILE, sect_df.index))
        exit()

    if args.action[0] == 'getcombinations3':
        print(get_ifr3_combinations(DBFILE, sect_df.index))
        exit()

    if args.action[0] == 'getcombinations3.1':
        print(get_ifr3_combinations(DBFILE,
                                    sect_df.index,
                                    icing_temp=2,
                                    outfile='output/ifr3.1-combinations.csv'))
        exit()

    if args.action[0] == 'getcombinations3.2':
        print(get_ifr3_combinations(DBFILE,
                                    sect_df.index,
                                    icing_temp=None,
                                    outfile='output/ifr3.2-combinations.csv'))
        exit()

    if args.action[0] == 'generate_ifr0':
        print(generate_scenario_ifr0(DBFILE, sect_df.index))
        exit()

    if args.action[0] == 'generate_ifr3':
        print(generate_scenario_ifr3(DBFILE, sect_df.index))
        exit()

    if args.action[0] == 'generate_ifr3.1':
        print(generate_scenario_ifr3(DBFILE,
                                     sect_df.index,
                                     icing_temp=2,
                                     sub_id = 1,
                                     outfile='output/HEMS_SCN_IFR3.1.xlsx'))
        exit()

    if args.action[0] == 'generate_ifr3.2':
        print(generate_scenario_ifr3(DBFILE,
                                     sect_df.index,
                                     icing_temp=None,
                                     sub_id = 2,
                                     outfile='output/HEMS_SCN_IFR3.2.xlsx'))
        exit()

    if args.action[0] == 'validate_ifr0':
        #limit = 2
        df = None
        from random import shuffle
        with pd.ExcelWriter("output/IFR0_validation.xlsx") as writer:
            sectors = list(sect_df.index)
            shuffle(sectors)
            #sectors = sectors[:limit]
            for i, sector in enumerate(sectors):
                print(f"Processing sector {sector} ({i+1}/{len(sectors)})")
                ifr_alts = []
                ifr_bool = sect_df.loc[sector, 'ifr_boolean']
                if not ifr_bool:
                    continue
                alts = sect_df.loc[sector, 'alt_ifr']
                ifr_alts = alts.split(';')
                apch_bool = int(sect_df.loc[sector, 'sect_apch_boolean'])
                chunk = validate_ifr0(DBFILE, sector, ifr_alts, apch_bool)
                chunk = chunk.reset_index(drop=True)
                chunk.insert(0, 'sector', sector)
                if df is None:
                    df = chunk
                else:
                    df = pd.concat([df, chunk])
            df.to_excel(writer, f'FHXX_sample', index=False, freeze_panes=(1, 0))
        exit()

    if args.action[0] == 'validate_ifr3':
        #limit = 2
        df = None
        from random import shuffle
        with pd.ExcelWriter("output/IFR3_validation.xlsx") as writer:
            sectors = list(sect_df.index)
            shuffle(sectors)
            #sectors = sectors[:limit]
            for i, sector in enumerate(sectors):
                print(f"Processing sector {sector} ({i+1}/{len(sectors)})")
                ifr_alts = []
                ifr_bool = sect_df.loc[sector, 'ifr_boolean']
                if not ifr_bool:
                    continue
                alts = sect_df.loc[sector, 'alt_ifr']
                ifr_alts = alts.split(';')
                apch_bool = int(sect_df.loc[sector, 'sect_apch_boolean'])
                chunk = validate_ifr3(DBFILE, sector, ifr_alts, apch_bool)
                chunk = chunk.reset_index(drop=True)
                chunk.insert(0, 'sector', sector)
                if df is None:
                    df = chunk
                else:
                    df = pd.concat([df, chunk])
            df.to_excel(writer, f'FHXX_sample', index=False, freeze_panes=(1, 0))


        exit()
