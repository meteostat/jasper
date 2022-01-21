"""
█▀▄▀█ █▀▀ ▀█▀ █▀▀ █▀█ █▀ ▀█▀ ▄▀█ ▀█▀
█░▀░█ ██▄ ░█░ ██▄ █▄█ ▄█ ░█░ █▀█ ░█░

Meteor

Automated routines for importing,
exporting and managing Meteostat data.

The code is licensed under the MIT license.
"""

__appname__ = 'meteor'
__version__ = '0.9.0'

from .meteor import Meteor
from .runner import run
