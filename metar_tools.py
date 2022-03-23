import logging
from metar import Metar
from metar_taf_parser.parser.parser import MetarParser

def parse(metar_str, year, month):
    return Metar.Metar(metar_str, year=year, month=month, strict=False)

def get_icao(metar):
    return metar.station_id

def get_temp(metar):
    if metar.temp is None:
        return None
    return int(metar.temp.value())

def get_vis(metar):
    if metar.vis is None: # try another parser
        try:
            m = MetarParser().parse(metar.code)
        except (ValueError, IndexError) as e:
            logging.error(str(e))
            return None
        if m.visibility is None:
            return None
        if not m.visibility.distance.endswith('m'):
            return None
        if m.visibility.distance == "> 10km":
            return 10000
        return int(m.visibility.distance.replace('m', ''))
    return int(metar.vis.value())

def get_rvr(metar):
    """Returns best runway visual range. If not reported, returns 2001.
       RVR is not reported if > 2000 m."""
    rvr = 2001
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
    #if len(metar.sky) == 0:
        # no cloud data available - set to 0? -No, this implies CAVOK.
        #metar_ceils.append(0)
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
            if cloud[1] is None:
                metar_bases.append(0)
                continue
            metar_bases.append(cloud[1].value())
    if len(metar_bases) > 0:
        base = min(metar_bases)
    return int(base)

def get_calc_base(metar):
    t = metar.temp.value()
    dew = metar.dewpt.value()
    return int(((t - dew) / 2.5) * 1000)
