"""The official python client for ConnectorDB.

To install the client::

    pip install connectordb

Another optional requirement is python-apsw.


The client enables quick usage of the database for IoT stuff and data analysis::

    import time
    import connectordb

    # Log in as a device
    cdb = connectordb.ConnectorDB("apikey")

    # Get the temperature stream
    temp = cdb["temperature"]

    if not temp.exists():
        temp.create({"type": "number"}) # connectordb streams use JSON schemas

    while True:
        time.sleep(1)
        t = get_temperature()
        temp.insert(t)

The client also allows anonymous access of database values if the database is configured
to allow public access::

    import connectordb
    cdb = connectordb.ConnectorDB()

    usr = cdb("myuser")

"""
from __future__ import absolute_import

from ._connectordb import *
from ._connection import AuthenticationError, ServerError
from ._datapointarray import DatapointArray

__version__ = "0.3.5"
