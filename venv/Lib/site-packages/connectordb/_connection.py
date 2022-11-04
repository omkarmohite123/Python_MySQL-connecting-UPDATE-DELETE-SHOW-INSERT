from __future__ import absolute_import

# python 3 vs 2
try:
    from urlparse import urljoin
except:
    from urllib.parse import urljoin

from requests import Session
from requests.auth import HTTPBasicAuth

import json

from ._websocket import WebsocketHandler

# The subpath to the Create Read Update Delete portion of the API
CRUD_PATH = "crud/"


# Returned when the given credentials are not accepted by the server
class AuthenticationError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


# Returned when the server gives an unhandled error code
class ServerError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class DatabaseConnection(object):

    def __init__(self, user_or_apikey=None, user_password=None, url="https://connectordb.com"):

        # Set up the API URL
        if not url.startswith("http"):
            url = "https://" + url
        if not url.endswith("/"):
            url = url + "/"
        self.baseurl = url
        self.url = urljoin(url, "/api/v1/")

        # Set up a session, which allows us to reuse connections
        self.r = Session()
        self.r.headers.update({'content-type': 'application/json'})

        # Prepare the websocket
        self.ws = WebsocketHandler(self.url, None)

        # Set the authentication if any
        self.setauth(user_or_apikey, user_password)

        # Now set up the login path so we know what we're logged in as
        if user_password is not None:
            self.path = user_or_apikey + "/user"
        else:
            self.path = self.ping()

    def setauth(self, user_or_apikey=None, user_password=None):
        """ setauth sets the authentication header for use in the session.
        It is for use when apikey is updated or something of the sort, such that
        there is a seamless experience. """
        auth = None
        if user_or_apikey is not None:
            # ConnectorDB allows login using both basic auth or an apikey url param.
            # The python client uses basic auth for all logins
            if user_password is None:
                # Login by api key - the basic auth login uses "" user and
                # apikey as password
                user_password = user_or_apikey
                user_or_apikey = ""
            auth = HTTPBasicAuth(user_or_apikey, user_password)
            self.r.auth = auth

        # Set the websocket's authentication
        self.ws.setauth(auth)

    def close(self):
        """Closes the active connections to ConnectorDB"""
        self.r.close()

    def handleresult(self, r):
        """Handles HTTP error codes for the given request

        Raises:
            AuthenticationError on the appropriate 4** errors
            ServerError if the response is not an ok (2**)

        Arguments:
            r -- The request result
        """
        if r.status_code >= 400 and r.status_code < 500:
            msg = r.json()
            raise AuthenticationError(str(msg["code"]) + ": " + msg["msg"] +
                                      " (" + msg["ref"] + ")")
        elif r.status_code > 300:
            err = None
            try:
                msg = r.json()
                err = ServerError(str(msg["code"]) + ": " + msg["msg"] + " (" +
                                  msg["ref"] + ")")
            except:
                raise ServerError(
                    "Server returned error, but did not give a valid error message")
            raise err
        return r

    def ping(self):
        """Attempts to ping the server using current credentials, and responds with the path of the currently
        authenticated device"""
        return self.handleresult(self.r.get(self.url,
                                            params={"q": "this"})).text

    def query(self, query_type, query=None):
        """Run the given query on the connection (POST request to /query)"""
        return self.handleresult(self.r.post(urljoin(self.url + "query/",
                                                     query_type),
                                             data=json.dumps(query))).json()

    def create(self, path, data=None):
        """Send a POST CRUD API request to the given path using the given data which will be converted
        to json"""
        return self.handleresult(self.r.post(urljoin(self.url + CRUD_PATH,
                                                     path),
                                             data=json.dumps(data)))

    def read(self, path, params=None):
        """Read the result at the given path (GET) from the CRUD API, using the optional params dictionary
        as url parameters."""
        return self.handleresult(self.r.get(urljoin(self.url + CRUD_PATH,
                                                    path),
                                            params=params))

    def update(self, path, data=None):
        """Send an update request to the given path of the CRUD API, with the given data dict, which will be converted
        into json"""
        return self.handleresult(self.r.put(urljoin(self.url + CRUD_PATH,
                                                    path),
                                            data=json.dumps(data)))

    def delete(self, path):
        """Send a delete request to the given path of the CRUD API. This deletes the object. Or at least tries to."""
        return self.handleresult(self.r.delete(urljoin(self.url + CRUD_PATH,
                                                       path)))

    def get(self, path, params=None):
        """Sends a get request to the given path in the database and with optional URL parameters"""
        return self.handleresult(self.r.get(urljoin(self.url, path),
                                            params=params))

    def subscribe(self, stream, callback, transform=""):
        """Subscribe to the given stream with the callback"""
        return self.ws.subscribe(stream, callback, transform)

    def unsubscribe(self, stream, transform=""):
        """Unsubscribe from the given stream"""
        return self.ws.unsubscribe(stream, transform)

    def wsdisconnect(self):
        """Disconnects the websocket"""
        self.ws.disconnect()
