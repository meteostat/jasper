from numpy import isnan

def temp(value):

    if isnan(value) or value == None:
        return None
    elif value >= -100 and value <= 65:
        return value
    else:
        return None

def rhum(value):

    if isnan(value) or value == None:
        return None
    if value >= 0 and value <= 100:
        return value
    else:
        return None

def prcp_hourly(value):

    if isnan(value) or value == None:
        return None
    elif value >= 0 and value <= 350:
        return value
    else:
        return None

def wspd(value):

    if isnan(value) or value == None:
        return None
    elif value >= 0 and value <= 250:
        return value
    else:
        return None

def wpgt(value):

    if isnan(value) or value == None:
        return None
    elif value >= 0 and value <= 500:
        return value
    else:
        return None

def wdir(value):

    if isnan(value) or value == None:
        return None
    elif value >= 0 and value <= 360:
        return value
    else:
        return None

def pres(value):

    if isnan(value) or value == None:
        return None
    elif value >= 850 and value <= 1090:
        return value
    else:
        return None
