from __future__ import absolute_import
import json
import os

from ._connection import DatabaseConnection

from ._device import Device
from ._user import User
from ._stream import Stream, DATAPOINT_INSERT_LIMIT

# By default assume that a ConnectorDB server is running on localhost with
# default configuration
CONNECTORDB_URL = "http://localhost:3124"


class ConnectorDB(Device):
    """ConnectorDB is the main entry point for any application that uses the python API.
    The class accepts both a username and password in order to log in as a user, and accepts an apikey
    when logging in directly from a device::

        import connectordb
        cdb = connectordb.ConnectorDB("myusername","mypassword")

        #prints "myusername/user" - logging in by username/password combo
        #logs in as the user device.
        print cdb.path

    """

    def __init__(self, user_or_apikey=None, user_password=None, url=CONNECTORDB_URL):

        db = DatabaseConnection(user_or_apikey, user_password, url)

        # ConnectorDB uses bcrypt by default for password hashing. While great for security
        # of passwords, it is extremely expensive, so it slows down queries. So, if we logged in
        # as a user with password, attempt to get the user device apikey to use for future authentication
        # so that queries are fast
        if user_password is not None:
            # Logins happen as a user device
            Device.__init__(self, db, user_or_apikey + "/user")

            if self.apikey is not None:
                # Reset the auth to be apikey
                db.setauth(self.apikey)
        else:
            # We logged in as a device - we have to ping the server to get our
            # name
            Device.__init__(self, db, db.path)

    def __call__(self, path):
        """Enables getting arbitrary users/devices/streams in a simple way. Just call the object
        with the u/d/s uri
            cdb = ConnectorDB("myapikey")
            cdb("user1") -> user1 object
            cdb("user1/device1") -> user1/device1 object
            cdb("user1/device1/stream1") -> user1/device1/stream1 object
        """
        n = path.count("/")
        if n == 0:
            return User(self.db, path)
        elif n == 1:
            return Device(self.db, path)
        else:
            return Stream(self.db, path)

    def close(self):
        """shuts down all active connections to ConnectorDB"""
        self.db.close()

    def reset_apikey(self):
        """invalidates the device's current api key, and generates a new one. Resets current auth to use the new apikey,
        since the change would have future queries fail if they use the old api key."""
        apikey = Device.reset_apikey(self)
        self.db.setauth(apikey)
        return apikey

    def count_users(self):
        """Gets the total number of users registered with the database. Only available to administrator."""
        return int(self.db.get("", {"q": "countusers"}).text)

    def count_devices(self):
        """Gets the total number of devices registered with the database. Only available to administrator."""
        return int(self.db.get("", {"q": "countdevices"}).text)

    def count_streams(self):
        """Gets the total number of streams registered with the database. Only available to administrator."""
        return int(self.db.get("", {"q": "countstreams"}).text)

    def info(self):
        """returns a dictionary of information about the database, including the database version, the transforms
        and the interpolators supported::

            >>>cdb = connectordb.ConnectorDB(apikey)
            >>>cdb.info()
            {
                "version": "0.3.0",
                "transforms": {
                    "sum": {"description": "Returns the sum of all the datapoints that go through the transform"}
                    ...
                },
                "interpolators": {
                    "closest": {"description": "Uses the datapoint closest to the interpolation timestamp"}
                    ...
                }
            }

        """
        return {
            "version": self.db.get("meta/version").text,
            "transforms": self.db.get("meta/transforms").json(),
            "interpolators": self.db.get("meta/interpolators").json()
        }

    def __repr__(self):
        return "[ConnectorDB:%s]" % (self.path, )

    def users(self):
        """Returns the list of users in the database"""
        result = self.db.read("", {"q": "ls"})

        if result is None or result.json() is None:
            return []
        users = []
        for u in result.json():
            usr = self(u["name"])
            usr.metadata = u
            users.append(usr)
        return users

    def ping(self):
        """Pings the ConnectorDB server. Useful for checking if the connection is valid"""
        return self.db.ping()

    def import_users(self, directory):
        """Imports version 1 of ConnectorDB export. These exports can be generated
        by running user.export(dir), possibly on multiple users.
        """
        exportInfoFile = os.path.join(directory, "connectordb.json")
        with open(exportInfoFile) as f:
            exportInfo = json.load(f)
        if exportInfo["Version"] != 1:
            raise ValueError("Not able to read this import version")

        # Now we list all the user directories
        for name in os.listdir(directory):
            udir = os.path.join(directory, name)
            if os.path.isdir(udir):
                # Let's read in the user
                with open(os.path.join(udir, "user.json")) as f:
                    usrdata = json.load(f)

                u = self(usrdata["name"])
                if u.exists():
                    raise ValueError("The user " + name + " already exists")

                del usrdata["name"]
                u.create(password=name, **usrdata)

                # Now read all of the user's devices
                for dname in os.listdir(udir):
                    ddir = os.path.join(udir, dname)
                    if os.path.isdir(ddir):
                        u.import_device(ddir)
