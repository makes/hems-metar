from datetime import datetime
from datetime import timedelta
import pysolar.solar as solar

TIME_DAY = 0
TIME_NIGHT = 1
TIME_SUNSET = 2

def get_time_of_day(timestamp: str, lat: float, lon: float):
    """
    Returns 0 if daytime, 1 if night or sunset within 30 minutes, 2 if sunset between 30 and 60 mins.

    :param str timestamp: UTC time in format '%Y-%m-%d %H:%M:%S'.
    :param float lat: Latitude of the location.
    :param float lon: Longitude of the location.
    :return: TIME_DAY=0, TIME_NIGHT=1, TIME_SUNSET=2
    :rtype: int
    """
    dt = datetime.fromisoformat(timestamp + '+00:00')
    alt = solar.get_altitude(lat, lon, dt)
    if alt < -6:
        return TIME_NIGHT
    else:
        alt60 = solar.get_altitude(lat, lon, dt + timedelta(minutes=60))
        if alt60 >= -6:
            return TIME_DAY
        else:
            alt30 = solar.get_altitude(lat, lon, dt + timedelta(minutes=30))
            if alt30 >= -6:
                return TIME_SUNSET
            else:
                return TIME_NIGHT

def check_sundown_vfr(row):
    if row['vis_term'] >= 3000 and row['taf_vis_term'] >= 3000 and row['base_term'] >= 1200: # taf_base_term >= 1200?
        return 'SUNDOWN HEMS VFR'
    else:
        return 'SUNDOWN BELOW VFR'

def check_vfr(row):
    # test NIGHT HEMS VFR
    if row['time_of_day'] == TIME_NIGHT:
        if row['vis_term'] >= 3000 and row['vis_sect'] >= 3000 and row['taf_vis_term'] >= 3000:
            if row['ceil_term'] >= 1200 and row['ceil_sect'] >= 1200:
                if row['base_term'] < 1200 or row['base_sect'] < 1200:
                    return 'NIGHT VFR HEMS FEW CLOUD'
                else:
                    return 'NIGHT HEMS VFR'
        return 'NIGHT BELOW VFR'

    # test DAY HEMS VFR
    if row['vis_term'] >= 3000 and row['vis_sect'] >= 3000 and row['taf_vis_term'] >= 3000:
        if row['ceil_term'] >= 300 and row['ceil_sect'] >= 300:
            if row['time_of_day'] != TIME_SUNSET:
                return 'DAY HEMS VFR'
            else:
                return check_sundown_vfr(row)
    if row['vis_term'] >= 2000 and row['vis_sect'] >= 2000:
        if row['ceil_term'] >= 400 and row['ceil_sect'] >= 400:
            if row['time_of_day'] != TIME_SUNSET:
                return 'DAY HEMS VFR'
            else:
                return check_sundown_vfr(row)
    if row['vis_term'] >= 500 and row['vis_sect'] >= 500:
        if row['ceil_term'] >= 500 and row['ceil_sect'] >= 500:
            if row['time_of_day'] != TIME_SUNSET:
                return 'DAY HEMS VFR'
            else:
                return check_sundown_vfr(row)
    return 'DAY BELOW VFR'

#def classify_vfr(lat, lon, time_of_day,
#                 vis_term, ceil_term, base_term,
#                 taf_vis_term,
#                 vis_sect, ceil_sect, base_sect):
#    #time_of_day = get_time_of_day(time, lat, lon)
#    # test NIGHT HEMS VFR
#    if time_of_day != TIME_DAY:
#        if vis_term >= 3000 and vis_sect >= 3000 and taf_vis_term >= 3000:
#            if base_term >= 1200 and base_sect >= 1200:
#                return 'NIGHT HEMS VFR'
#            #else:
#                #return 'NIGHT VFR HEMS FEW CLOUD'
#        return None
#
#    # test DAY HEMS VFR
#    if vis_term >= 3000 and vis_sect >= 3000:
#        return 'DAY HEMS VFR'
#    if vis_term >= 2000 and vis_sect >= 2000:
#        if ceil_term >= 400 and ceil_sect >= 400:
#            return 'DAY HEMS VFR'
#    if vis_term >= 500 and vis_sect >= 500:
#        if ceil_term >= 500 and ceil_sect >= 500:
#            return 'DAY HEMS VFR'
#    return None

def find_ifr_alt(alts, alt_df, time, rvr_factor):
    icing = False
    for alt in alts:
        #alt_row = alt_df[(alt_df.icao == alt) & (alt_df.time == time)]
        try:
            alt_row = alt_df.loc[(alt, time)]
        except KeyError:
            continue
        alt_row = alt_row.iloc[0]
        taf_rvr = int(alt_row['taf_vis'] * rvr_factor)
        if alt_row['rvr'] >= 900 and alt_row['ceil'] >= 400 and taf_rvr >= 900:
            if alt_row['temp'] < 7:
                icing = True
            else:
                return alt
    return icing

def check_ifr(row, alt_df, alts, cld_ceiling, ifr_return_only=False):
    rvr_factor = 1.5
    if row['time_of_day'] != TIME_DAY:
        rvr_factor = 2
    taf_rvr = int(row['taf_vis_term'] * rvr_factor)

    if row['rvr_term'] < 500 or taf_rvr < 500:
        return 'DAY BELOW IFR' if row['time_of_day'] == TIME_DAY else 'NIGHT BELOW IFR'

    if row['vfr_class'] != 'SUNDOWN BELOW VFR':
        if row['vis_sect'] < 3000 or row['ceil_sect'] < cld_ceiling:
            return 'DAY IFR NO CLOUD BREAK' if row['time_of_day'] == TIME_DAY else 'NIGHT IFR NO CLOUD BREAK'

    if row['temp_term'] < 7:
        return 'DAY IFR ICING' if row['time_of_day'] == TIME_DAY else 'NIGHT IFR ICING'

    alt = find_ifr_alt(alts, alt_df, row['time'], rvr_factor)
    #alt = alts[0]
    if type(alt) == bool:
        if alt == True:
            return 'DAY IFR ICING' if row['time_of_day'] == TIME_DAY else 'NIGHT IFR ICING'
        else:
            return 'DAY IFR NO ALTERNATE' if row['time_of_day'] == TIME_DAY else 'NIGHT IFR NO ALTERNATE'

    if row['vfr_class'] == 'SUNDOWN BELOW VFR':
        return 'DAY HEMS VFR RETURN NIGHT IFR'

    return 'DAY HEMS IFR' if row['time_of_day'] == TIME_DAY else 'NIGHT HEMS IFR'


#def classify_hems(row, sect_df, geo_df, alt_col='alt_open'):
#    sect_name = row['sect_name']
#    icao_term = row['icao_term']
#    time = row['time']
#    lat = geo_df.loc[icao_term,'LAT']
#    lon = geo_df.loc[icao_term,'LON']
#    time_of_day = get_time_of_day(time, lat, lon)
#    alts = sect_df.loc[sect_name,alt_col].split(';')
#    cld_ceiling = sect_df.loc[sect_name,'cld_ceiling']
#    vfr_class = check_vfr(row, time_of_day)
#    #if hems_class is not None:
#    #    return hems_class
#    ifr_class = check_ifr(row, time_of_day, alts, cld_ceiling)
#    return vfr_class, ifr_class

if __name__ == "__main__":
    efhk_lat = 60.3183
    efhk_lon = 24.9497
    efhk_twil_fm = "2022-01-01 06:28:00"
    efhk_twil_to = "2022-01-01 14:20:00"
    day1 = "2022-01-01 12:20:00"
    day2 = "2022-01-01 13:19:00"
    day3 = "2022-01-01 06:29:00"
    sunset1 = "2022-01-01 13:21:00"
    sunset2 = "2022-01-01 13:49:00"
    night1 = "2022-01-01 13:51:00"
    night2 = "2022-01-01 15:00:00"
    night3 = "2022-01-01 06:27:00"
    assert(get_time_of_day(day2, efhk_lat, efhk_lon) == TIME_DAY)
    assert(get_time_of_day(day1, efhk_lat, efhk_lon) == TIME_DAY)
    assert(get_time_of_day(day3, efhk_lat, efhk_lon) == TIME_DAY)
    assert(get_time_of_day(sunset1, efhk_lat, efhk_lon) == TIME_SUNSET)
    assert(get_time_of_day(sunset2, efhk_lat, efhk_lon) == TIME_SUNSET)
    assert(get_time_of_day(night1, efhk_lat, efhk_lon) == TIME_NIGHT)
    assert(get_time_of_day(night2, efhk_lat, efhk_lon) == TIME_NIGHT)
    assert(get_time_of_day(night3, efhk_lat, efhk_lon) == TIME_NIGHT)