from numpy import isnan


def temp(value):

    if value is None or isnan(value) or value < -100 or value > 65:
        return None
    else:
        return value


def rhum(value):

    if value is None or isnan(value) or value < 0 or value > 100:
        return None
    else:
        return value


def prcp_hourly(value):

    if value is None or isnan(value) or value < 0 or value > 350:
        return None
    else:
        return value


def prcp_daily(value):

    if value is None or isnan(value) or value < 0 or value > 2000:
        return None
    else:
        return value

def prcp_monthly(value):

    if value is None or isnan(value) or value < 0 or value > 10000:
        return None
    else:
        return value


def wspd(value):

    if value is None or isnan(value) or value < 0 or value > 250:
        return None
    else:
        return value


def wpgt(value):

    if value is None or isnan(value) or value < 0 or value > 500:
        return None
    else:
        return value


def wdir(value):

    if value is None or isnan(value) or value < 0 or value > 360:
        return None
    else:
        return value


def pres(value):

    if value is None or isnan(value) or value < 850 or value > 1090:
        return None
    else:
        return value


def snow(value):

    if value is None or isnan(value) or value < 0 or value > 11000:
        return None
    else:
        return value


def tsun_hourly(value):

    if value is None or isnan(value) or value < 0 or value > 60:
        return None
    else:
        return value


def tsun_daily(value):

    if value is None or isnan(value) or value < 0 or value > 1440:
        return None
    else:
        return value

def tsun_monthly(value):

    if value is None or isnan(value) or value < 0 or value > 44640:
        return None
    else:
        return value


def coco(value):

    if value is None or isnan(value) or value < 0 or value > 27:
        return None
    else:
        return value
