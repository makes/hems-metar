import pandas as pd

def read_sector_df(con, sect_name):
    sql = """
    SELECT sect.Sector_no AS sect_name, metar_term.time AS time, sect.dep_dest AS icao_term, sect.sect_wx AS icao_sect,
        metar_term.temp AS temp_term, metar_term.vis AS vis_term, metar_term.rvr AS rvr_term, metar_term.ceil AS ceil_term, metar_term.base AS base_term, metar_term.metar_msg AS metar_msg_term,
        taf_term.taf_vis AS taf_vis_term, taf_term.taf_msg AS taf_msg_term,
        metar_sect.temp AS temp_sect, metar_sect.vis AS vis_sect, metar_sect.rvr AS rvr_sect, metar_sect.ceil AS ceil_sect, metar_sect.base AS base_sect, metar_sect.metar_msg AS metar_msg_sect,
        metar_term.time_of_day AS time_of_day
    FROM sect
    INNER JOIN metar metar_term
    ON icao_term = metar_term.icao
    INNER JOIN metar metar_sect
    ON icao_sect = metar_sect.icao AND metar_term.time = metar_sect.time
    LEFT JOIN taf taf_term
    ON icao_term = taf_term.icao AND metar_term.time = taf_term.time
    WHERE sect_name = :sect_name
    """
    df = pd.read_sql(sql, con, params={'sect_name': sect_name})
    return df

def read_alt_df(con, alts):
    sql = """
    SELECT metar.icao AS icao, metar.time as time, metar.temp AS temp, metar.rvr AS rvr, metar.ceil AS ceil, taf.taf_vis AS taf_vis
    FROM metar
    LEFT JOIN taf
    ON metar.icao = taf.icao AND metar.time = taf.time
    """
    sql += f"WHERE metar.icao in ({','.join(['?']*len(alts))}) "
    sql += "ORDER BY time, icao"

    df = pd.read_sql(sql, con, params=alts, index_col=['icao', 'time']).dropna()
    return df.sort_index()

def make_hourly_csv(df, sect_name):
    #hourly = pd.DataFrame(df.sect_name.groupby([df.month, df.hour])).mean()
    hourly = pd.DataFrame(df.VFR.groupby([df.month, df.hour]).sum() / df.groupby([df.month, df.hour]).size(), columns=['VFR'])
    #hourly['VFR'] = df.VFR.groupby([df.month, df.hour]).sum() / df.groupby([df.month, df.hour]).size()
    hourly['IFR'] = df.IFR.groupby([df.month, df.hour]).sum() / df.groupby([df.month, df.hour]).size()
    hourly['NO FLY ICING'] = df['NO FLY ICING'].groupby([df.month, df.hour]).sum() / df.groupby([df.month, df.hour]).size()
    hourly['NO FLY'] = df['NO FLY'].groupby([df.month, df.hour]).sum() / df.groupby([df.month, df.hour]).size()
    #canfly = canfly.drop('can_fly', axis=1)
    hourly.insert(0, 'sector', sect_name)
    return hourly

def make_hourly_csv_vfr_only(df, sect_name):
    hourly = pd.DataFrame(df.VFR.groupby([df.month, df.hour]).sum() / df.groupby([df.month, df.hour]).size(), columns=['VFR'])
    hourly.insert(0, 'sector', sect_name)
    return hourly