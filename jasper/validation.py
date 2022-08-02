"""
Performe simple quality checks on meteorological data

The code is licensed under the MIT license.
"""

from numpy import isnan


def temp(value):
    """
    Validate all sorts of temperature data (Celsius)
    """
    try:
        if value is None or isnan(value) or value < -100 or value > 65:
            return None
        return value
    except BaseException:
        return None


def rhum(value):
    """
    Validate relative humidity data (percentage)
    """
    try:
        if value is None or isnan(value) or value < 0 or value > 100:
            return None
        return value
    except BaseException:
        return None


def prcp_hourly(value):
    """
    Validate hourly precipitation data (mm)
    """
    try:
        if value is None or isnan(value) or value < 0 or value > 350:
            return None
        return value
    except BaseException:
        return None


def prcp_daily(value):
    """
    Validate daily precipitation data (mm)
    """
    try:
        if value is None or isnan(value) or value < 0 or value > 2000:
            return None
        return value
    except BaseException:
        return None


def prcp_monthly(value):
    """
    Validate monthly precipitation data (mm)
    """
    try:
        if value is None or isnan(value) or value < 0 or value > 10000:
            return None
        return value
    except BaseException:
        return None


def wspd(value):
    """
    Validate (average) wind speed data (km/h)
    """
    try:
        if value is None or isnan(value) or value < 0 or value > 250:
            return None
        return value
    except BaseException:
        return None


def wpgt(value):
    """
    Validate peak wind gust data (km/h)
    """
    try:
        if value is None or isnan(value) or value < 0 or value > 500:
            return None
        return value
    except BaseException:
        return None


def wdir(value):
    """
    Validate wind direction data (degrees)
    """
    try:
        if value is None or isnan(value) or value < 0 or value > 360:
            return None
        return value
    except BaseException:
        return None


def pres(value):
    """
    Validate MSL air pressure data (hPa)
    """
    try:
        if value is None or isnan(value) or value < 850 or value > 1090:
            return None
        return value
    except BaseException:
        return None


def snow(value):
    """
    Validate snow cover data (mm)
    """
    try:
        if value is None or isnan(value) or value < 0 or value > 11000:
            return None
        return value
    except BaseException:
        return None


def tsun_hourly(value):
    """
    Validate hourly sunshine duration data (minutes)
    """
    try:
        if value is None or isnan(value) or value < 0 or value > 60:
            return None
        return value
    except BaseException:
        return None


def tsun_daily(value):
    """
    Validate daily sunshine duration data (minutes)
    """
    try:
        if value is None or isnan(value) or value < 0 or value > 1440:
            return None
        return value
    except BaseException:
        return None


def tsun_monthly(value):
    """
    Validate monthly sunshine duration data (minutes)
    """

    try:
        if value is None or isnan(value) or value < 0 or value > 44640:
            return None
        return value
    except BaseException:
        return None

def srad_hourly(value):
    """
    Validate hourly global solar radiation data
    """
    try:
        if value is None or isnan(value) or value < 0 or value > 600:
            return None
        return value
    except BaseException:
        return None


def srad_daily(value):
    """
    Validate daily global solar radiation data
    """
    try:
        if value is None or isnan(value) or value < 0 or value > 5000:
            return None
        return value
    except BaseException:
        return None


def cldc(value):
    """
    Validate cloud cover (oktas)
    """
    try:
        if value is None or isnan(value) or value < 0 or value > 8:
            return None
        return value
    except BaseException:
        return None


def vsby(value):
    """
    Validate visibility (meters)
    """
    try:
        if value is None or isnan(value) or value < 0 or value > 9999:
            return None
        return value
    except BaseException:
        return None


def coco(value):
    """
    Validate weather condition codes
    """
    try:
        if value is None or isnan(value) or value < 0 or value > 27:
            return None
        return value
    except BaseException:
        return None
