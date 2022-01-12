from datetime import datetime
import pysolar.solar as solar

def get_temp(metar):
    return int(metar.temp.value())

def get_vis(metar):
    return int(metar.vis.value())

def get_rvr(metar):
    """Returns best runway visual range. If not reported, returns 9999."""
    rvr = 9999
    metar_rvrs = []
    for rwy in metar.runway:
        metar_rvrs.append(rwy[1].value())
    if len(metar_rvrs) > 0:
        rvr = max(metar_rvrs)
    return int(rvr)

def get_ceil(metar):
    """
    Returns cloud ceiling as minimum of BKN, OVC and VV. If none of these apply, returns 9999.
    If indefinite (VV///, misty weather), returns 0.
    If no cloud data is available, returns 0.
    """
    ceil = 9999
    metar_ceils = []
    if len(metar.sky) == 0:
        # no cloud data available - set to 0?
        metar_ceils.append(0)
    for cloud in metar.sky:
        if cloud[0] == 'BKN' or cloud[0] == 'OVC' or cloud[0] == 'VV':
            if cloud[1] is None:
                metar_ceils.append(0)
                continue
            metar_ceils.append(cloud[1].value())
    if len(metar_ceils) > 0:
        ceil = min(metar_ceils)
    return int(ceil)

def get_base(metar, ceil):
    base = 9999
    metar_bases = [ceil]
    for cloud in metar.sky:
        if cloud[0] == 'FEW' or cloud[0] == 'SCT':
            metar_bases.append(cloud[1].value())
    if len(metar_bases) > 0:
        base = min(metar_bases)
    return int(base)

def get_calc_base(metar):
    t = metar.temp.value()
    dew = metar.dewpt.value()
    return int(((t - dew) / 2.5) * 1000)

def is_night(timestamp: str, lat: float, lon: float):
    """
    Returns True if it is night time - i.e. solar altitude is below -6 degrees.

    :param str timestamp: UTC time in format '%Y-%m-%d %H:%M:%S'.
    :param float lat: Latitude of the location.
    :param float lon: Longitude of the location.
    :return: True if it is night time, False otherwise.
    :rtype: bool
    """
    dt = datetime.fromisoformat(timestamp + '+00:00')
    alt = solar.get_altitude(lat, lon, dt)
    return alt < -6