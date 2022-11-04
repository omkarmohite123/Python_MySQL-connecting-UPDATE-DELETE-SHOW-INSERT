from __future__ import absolute_import
import json
import os

from ._connection import DatabaseConnection
from ._connectorobject import ConnectorObject

from ._datapointarray import DatapointArray

class Device(ConnectorObject):

    def create(self, public=False, **kwargs):
        """Creates the device. Attempts to create private devices by default,
        but if public is set to true, creates public devices.

        You can also set other default properties by passing in the relevant information.
        For example, setting a device with the given nickname and description::

            dev.create(nickname="mydevice", description="This is an example")

        Furthermore, ConnectorDB supports creation of a device's streams immediately,
        which can considerably speed up device setup::

            dev.create(streams={
                "stream1": {"schema": '{\"type\":\"number\"}'}
            })

        Note that the schema must be encoded as a string when creating in this format.
        """
        kwargs["public"] = public
        self.metadata = self.db.create(self.path, kwargs).json()

    def streams(self):
        """Returns the list of streams that belong to the device"""
        result = self.db.read(self.path, {"q": "ls"})

        if result is None or result.json() is None:
            return []
        streams = []
        for s in result.json():
            strm = self[s["name"]]
            strm.metadata = s
            streams.append(strm)
        return streams

    def __getitem__(self, stream_name):
        """Gets the child stream by name"""
        return Stream(self.db, self.path + "/" + stream_name)

    def __repr__(self):
        """Returns a string representation of the device"""
        return "[Device:%s]" % (self.path, )

    def export(self, directory):
        """Exports the device to the given directory. The directory can't exist. 
        You can later import this device by running import_device on a user.
        """
        if os.path.exists(directory):
            raise FileExistsError(
                "The device export directory already exists")

        os.mkdir(directory)

        # Write the device's info
        with open(os.path.join(directory, "device.json"), "w") as f:
            json.dump(self.data, f)

        # Now export the streams one by one
        for s in self.streams():
            s.export(os.path.join(directory, s.name))

    def import_stream(self, directory):
        """Imports a stream from the given directory. You export the Stream
        by using stream.export()"""

        # read the stream's info
        with open(os.path.join(directory, "stream.json"), "r") as f:
            sdata = json.load(f)

        s = self[sdata["name"]]
        if s.exists():
            raise ValueError("The stream " + s.name + " already exists")

        # Create the stream empty first, so we can insert all the data without
        # worrying about schema violations or downlinks
        s.create()

        # Now, in order to insert data into this stream, we must be logged in as
        # the owning device
        ddb = DatabaseConnection(self.apikey, url=self.db.baseurl)
        d = Device(ddb, self.path)

        # Set up the owning device
        sown = d[s.name]

        # read the stream's info
        sown.insert_array(DatapointArray().loadExport(directory))

        # Now we MIGHT be able to recover the downlink data,
        # only if we are not logged in as the device that the stream is being inserted into
        # So we check. When downlink is true, data is inserted into the
        # downlink stream
        if (sdata["downlink"] and self.db.path != self.path):
            s.downlink = True
            with open(os.path.join(directory, "downlink.json"), "r") as f:
                s.insert_array(json.load(f))

        # And finally, update the device
        del sdata["name"]
        s.set(sdata)

    # -----------------------------------------------------------------------
    # Following are getters and setters of the device's properties

    @property
    def apikey(self):
        """gets the device's api key. Returns None if apikey not accessible."""
        if "apikey" in self.data:
            return self.data["apikey"]
        return None

    def reset_apikey(self):
        """invalidates the device's current api key, and generates a new one"""
        self.set({"apikey": ""})
        return self.metadata["apikey"]

    @property
    def public(self):
        """gets whether the device is public
        (this means different things based on connectordb permissions setup - connectordb.com
        has this be whether the device is publically visible. Devices are individually public/private.)
        """
        if "public" in self.data:
            return self.data["public"]
        return None

    @public.setter
    def public(self, new_public):
        """Attempts to set whether the device is public"""
        self.set({"public": new_public})

    @property
    def role(self):
        """Gets the role of the device. This is the permissions level that the device has. It might
        not be accessible depending on the permissions setup of ConnectorDB. Returns None if not accessible"""
        if "role" in self.data:
            return self.data["role"]
        return None

    @role.setter
    def role(self, new_role):
        """ Attempts to set the device's role"""
        self.set({"role": new_role})

    @property
    def enabled(self):
        """ gets whether the device is enabled. This allows a device to notify ConnectorDB when
        it is active and when it is not running"""
        if "enabled" in self.data:
            return self.data["enabled"]
        return None

    @enabled.setter
    def enabled(self, new_enabled):
        """Sets the enabled state of the device"""
        self.set({"enabled": new_enabled})

    @property
    def user(self):
        """user returns the user which owns the given device"""
        return User(self.db, self.path.split("/")[0])


# The import has to go on the bottom because py3 imports are annoying
# about circular dependencies
from ._user import User
from ._stream import Stream
