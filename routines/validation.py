from numpy import isnan


def temp(value):

    try:

        if value is None or isnan(value) or value < -100 or value > 65:
            return None
        else:
            return value

    except BaseException:

        return None

def rhum(value):

    try:

        if value is None or isnan(value) or value < 0 or value > 100:
            return None
        else:
            return value

    except BaseException:

        return None

def prcp_hourly(value):

    try:

        if value is None or isnan(value) or value < 0 or value > 350:
            return None
        else:
            return value

    except BaseException:

        return None

def prcp_daily(value):

    try:

        if value is None or isnan(value) or value < 0 or value > 2000:
            return None
        else:
            return value

    except BaseException:

        return None

def prcp_monthly(value):

    try:

        if value is None or isnan(value) or value < 0 or value > 10000:
            return None
        else:
            return value

    except BaseException:

        return None

def wspd(value):

    try:

        if value is None or isnan(value) or value < 0 or value > 250:
            return None
        else:
            return value

    except BaseException:

        return None

def wpgt(value):

    try:

        if value is None or isnan(value) or value < 0 or value > 500:
            return None
        else:
            return value

    except BaseException:

        return None

def wdir(value):

    try:

        if value is None or isnan(value) or value < 0 or value > 360:
            return None
        else:
            return value

    except BaseException:

        return None

def pres(value):

    try:

        if value is None or isnan(value) or value < 850 or value > 1090:
            return None
        else:
            return value

    except BaseException:

        return None

def snow(value):

    try:

        if value is None or isnan(value) or value < 0 or value > 11000:
            return None
        else:
            return value

    except BaseException:

        return None

def tsun_hourly(value):

    try:

        if value is None or isnan(value) or value < 0 or value > 60:
            return None
        else:
            return value

    except BaseException:

        return None

def tsun_daily(value):

    try:

        if value is None or isnan(value) or value < 0 or value > 1440:
            return None
        else:
            return value

    except BaseException:

        return None

def tsun_monthly(value):

    try:

        if value is None or isnan(value) or value < 0 or value > 44640:
            return None
        else:
            return value

    except BaseException:

        return None

def coco(value):

    try:

        if value is None or isnan(value) or value < 0 or value > 27:
            return None
        else:
            return value

    except BaseException:

        return None
