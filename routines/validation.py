from numpy import isnan

def temp(value):

    if value == None or isnan(value):
        return None
    elif value >= -100 and value <= 65:
        return value
    else:
        return None

def rhum(value):

    if value == None or isnan(value):
        return None
    if value >= 0 and value <= 100:
        return value
    else:
        return None

def prcp_hourly(value):

    if value == None or isnan(value):
        return None
    elif value >= 0 and value <= 350:
        return value
    else:
        return None

def wspd(value):

    if value == None or isnan(value):
        return None
    elif value >= 0 and value <= 250:
        return value
    else:
        return None

def wpgt(value):

    if value == None or isnan(value):
        return None
    elif value >= 0 and value <= 500:
        return value
    else:
        return None

def wdir(value):

    if value == None or isnan(value):
        return None
    elif value >= 0 and value <= 360:
        return value
    else:
        return None

def pres(value):

    if value == None or isnan(value):
        return None
    elif value >= 850 and value <= 1090:
        return value
    else:
        return None
