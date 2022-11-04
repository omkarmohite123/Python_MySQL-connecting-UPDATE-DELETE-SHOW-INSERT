from __future__ import absolute_import

import datetime
import json
import os.path

class DatapointArray(list):
    """ Sometimes you might want to generate a stream by combining multiple disparate
    data sources. Since ConnectorDB streams currently only permit appending,
    a stream's data must be ordered correctly.

    The DatapointArray allows you to load all the data you'd need into a single object,
    and it merges the data together and formats it to be compatible with ConnectorDB
    """

    def __init__(self, data = []):
        """The data 

        """
        list.__init__(self,data)

    def __add__(self,other):
        return DatapointArray(self).merge(other)
    def __radd__(self,other):
        return DatapointArray(self).merge(other)

    def __getitem__(self,key):
        if (key=="t"):
            return self.t()
        if (key=="d"):
            return self.d()
        d = list.__getitem__(self,key)
        if isinstance(key, slice):
            d = DatapointArray(d)
            # If the data is unchanged, don't recompute the keys
            if not self._dataChanged:
                d._dataChanged = False
                d._d = self._d[key]
                d._t = self._t[key]
        return d

    def sort(self,f = lambda d: d["t"]):
        """Sort here works by sorting by timestamp by default"""
        list.sort(self,key=f)
        return self

    def d(self):
        """Returns just the data portion of the datapoints as a list"""
        return list(map(lambda x: x["d"],self.raw()))
    def t(self):
        """Returns just the timestamp portion of the datapoints as a list.
        The timestamps are in python datetime's date format."""
        return list(map(lambda x: datetime.datetime.fromtimestamp(x["t"]),self.raw()))

    def merge(self,array):
        """Adds the given array of datapoints to the generator.
        It assumes that the datapoints are formatted correctly for ConnectorDB, meaning
        that they are in the format::

            [{"t": unix timestamp, "d": data}]
        
        The data does NOT need to be sorted by timestamp - this function sorts it for you
        """
        self.extend(array)
        self.sort()

        return self

    def raw(self):
        """Returns array as a raw python array. For cases where for some reason
        the DatapointArray wrapper does not work for you

        """
        return list.__getitem__(self,slice(None,None))

    def writeJSON(self,filename):
        """Writes the data to the given file::

            DatapointArray([{"t": unix timestamp, "d": data}]).writeJSON("myfile.json")
        
        The data can later be loaded using loadJSON.
        """
        with open(filename, "w") as f:
            json.dump(self, f)

    def loadJSON(self,filename):
        """Adds the data from a JSON file. The file is expected to be in datapoint format::

            d = DatapointArray().loadJSON("myfile.json")
        """
        with open(filename, "r") as f:
            self.merge(json.load(f))
        return self

    def loadExport(self,folder):
        """Adds the data from a ConnectorDB export. If it is a stream export, then the folder
        is the location of the export. If it is a device export, then the folder is the export folder
        with the stream name as a subdirectory

        If it is a user export, you will use the path of the export folder, with the user/device/stream 
        appended to the end::

            myuser.export("./exportdir")
            DatapointArray().loadExport("./exportdir/username/devicename/streamname")
        """
        self.loadJSON(os.path.join(folder,"data.json"))
        return self

    def tshift(self,t):
        """Shifts all timestamps in the datapoint array by the given number of seconds.
        It is the same as the 'tshift' pipescript transform.

        Warning: The shift is performed in-place! This means that it modifies the underlying array::

            d = DatapointArray([{"t":56,"d":1}])
            d.tshift(20)
            print(d) # [{"t":76,"d":1}]
        """
        raw = self.raw()
        for i in range(len(raw)):
            raw[i]["t"] += t
        return self
    
    def sum(self):
        """Gets the sum of the data portions of all datapoints within"""
        raw = self.raw()
        s = 0
        for i in range(len(raw)):
            s += raw[i]["d"]
        return s
    
    def mean(self):
        """Gets the mean of the data portions of all datapoints within"""
        return self.sum()/float(len(self))
