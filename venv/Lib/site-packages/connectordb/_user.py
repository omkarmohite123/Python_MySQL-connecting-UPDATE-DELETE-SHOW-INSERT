from __future__ import absolute_import
import json
import os

from ._connectorobject import ConnectorObject


class User(ConnectorObject):

    def create(self, email, password, role="user", public=True, **kwargs):
        """Creates the given user - using the passed in email and password.

        You can also set other default properties by passing in the relevant information::

            usr.create("my@email","mypass",description="I like trains.")

        Furthermore, ConnectorDB permits immediate initialization of an entire user tree,
        so that you can create all relevant devices and streams in one go::

            usr.create("my@email","mypass",devices={
                "device1": {
                    "nickname": "My train",
                    "streams": {
                        "stream1": {
                            "schema": "{\"type\":\"string\"}",
                            "datatype": "train.choochoo"
                        }
                    },
                }
            })

        The user and meta devices are created by default. If you want to add streams to the user device,
        use the "streams" option in place of devices in create.
        """
        kwargs["email"] = email
        kwargs["password"] = password
        kwargs["role"] = role
        kwargs["public"] = public
        self.metadata = self.db.create(
            self.path, kwargs).json()

    def set_password(self, new_password):
        """Sets a new password for the user"""
        self.set({"password": new_password})

    def devices(self):
        """Returns the list of devices that belong to the user"""
        result = self.db.read(self.path, {"q": "ls"})

        if result is None or result.json() is None:
            return []
        devices = []
        for d in result.json():
            dev = self[d["name"]]
            dev.metadata = d
            devices.append(dev)
        return devices

    def streams(self, public=False, downlink=False, visible=True):
        """Returns the list of streams that belong to the user.
        The list can optionally be filtered in 3 ways:
            - public: when True, returns only streams belonging to public devices
            - downlink: If True, returns only downlink streams
            - visible: If True (default), returns only streams of visible devices
        """
        result = self.db.read(self.path, {"q": "streams",
                                          "public": str(public).lower(),
                                          "downlink": str(downlink).lower(),
                                          "visible": str(visible).lower()})

        if result is None or result.json() is None:
            return []
        streams = []
        for d in result.json():
            s = self[d["device"]][d["name"]]
            s.metadata = d
            streams.append(s)
        return streams

    def __getitem__(self, device_name):
        """Gets the child device by name"""
        return Device(self.db, self.path + "/" + device_name)

    def __repr__(self):
        """Returns a string representation of the user"""
        return "[User:%s]" % (self.path, )

    def export(self, directory):
        """Exports the ConnectorDB user into the given directory.
        The resulting export can be imported by using the import command(cdb.import(directory)),

        Note that Python cannot export passwords, since the REST API does
        not expose password hashes. Therefore, the imported user will have
        password same as username.

        The user export function is different than device and stream exports because
        it outputs a format compatible directly with connectorDB's import functionality:

            connectordb import < mydatabase > <directory >

        This also means that you can export multiple users into the same directory without issue
        """

        exportInfoFile = os.path.join(directory, "connectordb.json")
        if os.path.exists(directory):
            # Ensure that there is an export there already, and it is version 1
            if not os.path.exists(exportInfoFile):
                raise FileExistsError(
                    "The export directory already exsits, and is not a ConnectorDB export.")
            with open(exportInfoFile) as f:
                exportInfo = json.load(f)
            if exportInfo["Version"] != 1:
                raise ValueError(
                    "Could not export to directory: incompatible export versions.")
        else:
            # The folder doesn't exist. Make it.
            os.mkdir(directory)

            with open(exportInfoFile, "w") as f:
                json.dump(
                    {"Version": 1, "ConnectorDB": self.db.get("meta/version").text}, f)

        # Now we create the user directory
        udir = os.path.join(directory, self.name)
        os.mkdir(udir)

        # Write the user's info
        with open(os.path.join(udir, "user.json"), "w") as f:
            json.dump(self.data, f)

        # Now export the devices one by one
        for d in self.devices():
            d.export(os.path.join(udir, d.name))

    def import_device(self, directory):
        """Imports a device from the given directory. You export the device
        by using device.export()

        There are two special cases: user and meta devices.
        If the device name is meta, import_device will not do anything.
        If the device name is "user", import_device will overwrite the user device
        even if it exists already.
        """

        # read the device's info
        with open(os.path.join(directory, "device.json"), "r") as f:
            ddata = json.load(f)

        d = self[ddata["name"]]

        dname = ddata["name"]
        del ddata["name"]

        if dname == "meta":
            return
        elif dname == "user":
            d.set(ddata)
        elif d.exists():
            raise ValueError("The device " + d.name + " already exists")
        else:
            d.create(**ddata)

        # Now import all of the streams
        for name in os.listdir(directory):
            sdir = os.path.join(directory, name)
            if os.path.isdir(sdir):
                d.import_stream(sdir)

    # -----------------------------------------------------------------------
    # Following are getters and setters of the user's properties

    @property
    def email(self):
        """gets the user's email address"""
        if "email" in self.data:
            return self.data["email"]
        return None

    @email.setter
    def email(self, new_email):
        """sets the user's email address"""
        self.set({"email": new_email})

    @property
    def public(self):
        """gets whether the user is public
        (this means different things based on connectordb permissions setup - connectordb.com
        has this be whether the user is publically visible. Devices are individually public / private.)
        """
        if "public" in self.data:
            return self.data["public"]
        return None

    @public.setter
    def public(self, new_public):
        """Attempts to set whether the user is public"""
        self.set({"public": new_public})

    @property
    def role(self):
        """Gets the role of the user. This is the permissions level that the user has. It might
        not be accessible depending on the permissions setup of ConnectorDB. Returns None if not accessible"""
        if "role" in self.data:
            return self.data["role"]
        return None

    @role.setter
    def role(self, new_role):
        """ Attempts to set the user's role"""
        self.set({"role": new_role})

# The import has to go on the bottom because py3 imports are annoying
from ._device import Device
