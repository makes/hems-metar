from datetime import datetime
from datetime import timedelta
import logging
from metar_taf_parser.parser.parser import TAFParser
from metar_taf_parser.model.model import TAFTrend
from metar_taf_parser.model.model import TAF
from metar_taf_parser.model.enum import WeatherChangeType

def parse(taf_str):
    """
    Parses a TAF string.

    :param taf_str: TAF string.
    :return: Parsed TAF.
    :rtype: TAF
    """
    return TAFParser().parse('TAF ' + taf_str)

def validity_during_flight(taf, start: datetime, duration: int, deteriorating=False):
    """
    Returns 0 if forecast is not valid during flight, 1 if valid during part
    of the flight and 2 if valid during entire flight.

    :param taf: Parsed TAF or TAFTrend.
    :param start: Start time of flight (UTC).
    :param duration: Duration of flight in hours.
    :return: 0 if forecast is not valid during flight, 1 if valid during part
        of the flight and 2 if valid during entire flight.
    :rtype: int
    """

    # TAF can be valid between 6 to 30 hours
    # TAF contains no information on month or year
    end = start + timedelta(hours=duration)

    taf_type = None
    if isinstance(taf, TAF):
        taf_type = 'TAF'
    elif isinstance(taf, TAFTrend):
        if taf.type == WeatherChangeType.TEMPO:
            taf_type = 'TEMPO'
        elif taf.type == WeatherChangeType.BECMG:
            taf_type = 'BECMG'
    if taf_type is None:
        return 0

    # scan for start timestamp
    taf_start = datetime(year=start.year, month=start.month, day=start.day, hour=start.hour)
    cnt = 0
    direction = -1
    try:
        validity = taf.validity
    except AttributeError:
        logging.log(logging.WARN, 'TAF does not have validity attribute')
        return 0
    while taf_start.day != taf.validity.start_day or taf_start.hour != taf.validity.start_hour:
        taf_start = taf_start + timedelta(hours=1*direction)
        cnt += 1
        if cnt >= 30:
            if direction == -1:
                direction = 1
                cnt = 0
                taf_start = datetime(year=start.year, month=start.month, day=start.day, hour=start.hour)
            else:
                return 0

    # scan for end timestamp
    taf_end = taf_start
    cnt = 0
    while taf_end.day != taf.validity.end_day or taf_end.hour != taf.validity.end_hour:
        # end_hour can be 24!
        if taf.validity.end_hour == 24 and taf_end.hour == 0:
            if (taf_end - timedelta(hours=1)).day == taf.validity.end_day:
                break
        taf_end = taf_end + timedelta(hours=1)
        if cnt >= 30:
            logging.warning("TAF end timestamp scanning failed")
            return 0

    if taf_type == 'TEMPO':
        if not deteriorating:
            return 0 # ignore temporary weather improvement

    if taf_type == 'BECMG':
        if taf_end <= start:
            return 2
        if deteriorating and taf_start < end:
            return 1 # transision period is already considered bad weather
                     # if deteriorating.
        else:
            return 0

    # TAF or TEMPO
    if taf_start <= start and taf_end >= end:
        return 2
    elif taf_start >= start and taf_end <= end:
        return 1
    elif taf_start <= start and taf_end > start:
        return 1
    elif taf_start < end and taf_end >= end:
        return 1
    else:
        return 0

def get_vis(taf):
    """
    Returns visibility in meters.

    :param taf: Parsed TAF or TAFTrend.
    :return: Visibility in meters.
    :rtype: int
    """
    if taf.visibility is None:
        return None
    if not taf.visibility.distance.endswith('m'):
        return None
    if taf.visibility.distance == "> 10km":
        return 10000
    return int(taf.visibility.distance.replace('m', ''))

def get_ceil(taf):
    """
    Returns cloud ceiling.

    :param taf: Parsed TAF or TAFTrend.
    :return: Cloud ceiling.
    :rtype: int
    """
    heights = []
    if not taf.clouds and not taf.vertical_visibility:
        return None
    if taf.vertical_visibility:
        heights.append(taf.vertical_visibility)
    if taf.clouds:
        for cloud in taf.clouds:
            if cloud.quantity.name in ['BKN', 'OVC']:
                heights.append(cloud.height)
    if not heights:
        return None
    return min(heights)

def get_worstcase_ceil(taf, start: str, duration: int):
    pass

def get_worstcase_vis(taf, start: str, duration: int):
    """
    Worst visibility conditions during flight.

    :param taf: Parsed TAF or TAFTrend.
    :param start: Start time of flight in ISO format (UTC).
    :param duration: Duration of flight in hours.
    :return: Worst-case visibility in meters
    :rtype: int
    """
    start = datetime.fromisoformat(start) # + '+00:00') (use offset-naive datetime for pandas compatibility)
    taf_vis = []
    if validity_during_flight(taf, start, duration) != 2:
        return None
    initial_vis = get_vis(taf)
    if initial_vis is not None:
        taf_vis.append(initial_vis)
    for trend in taf.trends:
        vis = get_vis(trend)
        if vis is None:
            continue
        if initial_vis is None:
            initial_vis = 10001
        deteriorating = vis < initial_vis
        valid = validity_during_flight(trend, start, duration, deteriorating=deteriorating)
        if valid == 0:
            continue
        if valid == 2:
            taf_vis = []
        taf_vis.append(vis)
    if len(taf_vis) == 0:
        return None
    return min(taf_vis)
