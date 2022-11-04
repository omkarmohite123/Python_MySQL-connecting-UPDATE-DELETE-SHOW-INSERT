from __future__ import absolute_import

import websocket
import threading
import logging
import json
import random
import time


class WebsocketHandler(object):
    """WebsocketHandler handles websocket connections to a ConnectorDB server. It allows
    subscribing and unsubscribing from inputs/outputs. The handler also deals with dropped
    connections, repeatedly attempting to reconnect to the server whenever connection is lost."""
    """The maximum time to wait between reconnection attempts"""
    reconnect_time_max_seconds = 8 * 60.0
    """Multiply the wait time by this factor when a reconnect fails"""
    reconnect_time_backoff_multiplier = 1.5
    """The time in seconds to wait before an initial attempt at reconnecting"""
    reconnect_time_starting_seconds = 1.0
    """The time between pings that results in a connection timeout"""
    connection_ping_timeout = 60 * 2

    def __init__(self, server_url, basic_auth):
        """
        The handler is initialized as follows::
            from requests.auth import HTTPBasicAuth
            req = HTTPBasicAuth(username,password)
            ws = WebsocketHandler("https://connectordb.com",req)
        """

        # The websocket is at /api/v1/websocket, and the server_url includes the /api/v1/
        server_url += "websocket"

        # First we must get the websocket URI from the server URL
        self.ws_url = "wss://" + server_url[8:]
        if server_url.startswith("http://"):
            self.ws_url = "ws://" + server_url[7:]

        self.setauth(basic_auth)

        # Set up the variable which will hold all of the subscriptions
        self.subscriptions = {}
        self.subscription_lock = threading.Lock()

        # The server periodically sends ping messages during websocket connection.
        # we keep track of the pings so that we notice loss of connection
        self.lastpingtime = time.time()
        self.pingtimer = None

        # Now set up the websocket
        self.ws = None
        self.ws_thread = None  # The thread where the websocket runs
        self.ws_openlock = threading.Lock()
        self.ws_sendlock = threading.Lock()

        # Set up the websocket status
        self._status = "disconnected"
        self._status_lock = threading.Lock()

        # Set up the reconnect time
        self.reconnect_time = self.reconnect_time_starting_seconds

        # Set up the times that we were connected and disconnected. These allow for
        # setting up reconnect delays correctly
        self.connected_time = 0
        self.disconnected_time = 0

    def setauth(self,basic_auth):
        """ setauth can be used during runtime to make sure that authentication is reset.
        it can be used when changing passwords/apikeys to make sure reconnects succeed """
        self.headers = []
        # If we have auth
        if basic_auth is not None:
            # we use a cheap hack to get the basic auth header out of the auth object.
            # This snippet ends up with us having an array of the necessary headers
            # to perform authentication.
            class auth_extractor():
                def __init__(self):
                    self.headers = {}

            extractor = auth_extractor()
            basic_auth(extractor)

            for header in extractor.headers:
                self.headers.append("%s: %s" % (header, extractor.headers[header]))

    @property
    def status(self):
        status = ""
        with self._status_lock:
            status = self._status
        return status

    @status.setter
    def status(self, newstatus):
        with self._status_lock:
            self._status = newstatus
        logging.debug("ConnectorDB:WS:STATUS: %s", newstatus)

    def send(self, cmd):
        """Send the given command thru the websocket"""
        with self.ws_sendlock:
            self.ws.send(json.dumps(cmd))

    def insert(self, stream, data):
        """Insert the given datapoints into the stream"""
        self.send({"cmd": "insert", "arg": stream, "d": data})

    def subscribe(self, stream, callback, transform=""):
        """Given a stream, a callback and an optional transform, sets up the subscription"""
        if self.status == "disconnected" or self.status == "disconnecting" or self.status == "connecting":
            self.connect()
        if self.status is not "connected":
            return False
        logging.debug("Subscribing to %s", stream)

        self.send({"cmd": "subscribe", "arg": stream, "transform": transform})
        with self.subscription_lock:
            self.subscriptions[stream + ":" + transform] = callback
        return True

    def unsubscribe(self, stream, transform=""):
        """Unsubscribe from the given stream (with the optional transform)"""
        if self.status is not "connected":
            return False
        logging.debug("Unsubscribing from %s", stream)
        self.send(
            {"cmd": "unsubscribe",
             "arg": stream,
             "transform": transform})

        self.subscription_lock.acquire()
        del self.subscriptions[stream + ":" + transform]
        if len(self.subscriptions) is 0:
            self.subscription_lock.release()
            self.disconnect()
        else:
            self.subscription_lock.release()

    def connect(self):
        """Attempt to connect to the websocket - and returns either True or False depending on if
        the connection was successful or not"""

        # Wait for the lock to be available (ie, the websocket is not being used (yet))
        self.ws_openlock.acquire()
        self.ws_openlock.release()

        if self.status == "connected":
            return True  # Already connected
        if self.status == "disconnecting":
            # If currently disconnecting, wait a moment, and retry connect
            time.sleep(0.1)
            return self.connect()
        if self.status == "disconnected" or self.status == "reconnecting":
            self.ws = websocket.WebSocketApp(self.ws_url,
                                             header=self.headers,
                                             on_message=self.__on_message,
                                             on_ping=self.__on_ping,
                                             on_open=self.__on_open,
                                             on_close=self.__on_close,
                                             on_error=self.__on_error)
            self.ws_thread = threading.Thread(target=self.ws.run_forever)
            self.ws_thread.daemon = True

            self.status = "connecting"
            self.ws_openlock.acquire()
            self.ws_thread.start()

        self.ws_openlock.acquire()
        self.ws_openlock.release()

        return self.status == "connected"

    def disconnect(self):
        if self.status == "connected":
            self.status = "disconnecting"
            with self.subscription_lock:
                self.subscriptions = {}

            self.ws.close()
            self.__on_close(self.ws)

    def __reconnect(self):
        """This is called when a connection is lost - it attempts to reconnect to the server"""
        self.status = "reconnecting"

        # Reset the disconnect time after 15 minutes
        if self.disconnected_time - self.connected_time > 15 * 60:
            self.reconnect_time = self.reconnect_time_starting_seconds
        else:
            self.reconnect_time *= self.reconnect_time_backoff_multiplier

        if self.reconnect_time > self.reconnect_time_max_seconds:
            self.reconnect_time = self.reconnect_time_max_seconds

        # We want to add some randomness to the reconnect rate - necessary so that we don't pound the server
        # if it goes down
        self.reconnect_time *= 1 + random.uniform(-0.2, 0.2)

        if self.reconnect_time < self.reconnect_time_starting_seconds:
            self.reconnect_time = self.reconnect_time_starting_seconds

        logging.warn("ConnectorDB:WS: Attempting to reconnect in %fs",
                     self.reconnect_time)

        self.reconnector = threading.Timer(self.reconnect_time,
                                           self.__reconnect_fnc)
        self.reconnector.daemon = True
        self.reconnector.start()

    def __reconnect_fnc(self):
        """This function is called by reconnect after the time delay"""
        if self.connect():
            self.__resubscribe()
        else:
            self.__reconnect()

    def __resubscribe(self):
        """Send subscribe command for all existing subscriptions. This allows to resume a connection
        that was closed"""
        with self.subscription_lock:
            for sub in self.subscriptions:
                logging.debug("Resubscribing to %s", sub)
                stream_transform = sub.split(":", 1)
                self.send({
                    "cmd": "subscribe",
                    "arg": stream_transform[0],
                    "transform": stream_transform[1]
                })

    def __on_open(self, ws):
        """Called when the websocket is opened"""
        logging.debug("ConnectorDB: Websocket opened")

        # Connection success - decrease the wait time for next connection
        self.reconnect_time /= self.reconnect_time_backoff_multiplier

        self.status = "connected"

        self.lastpingtime = time.time()
        self.__ensure_ping()

        self.connected_time = time.time()

        # Release the lock that connect called
        self.ws_openlock.release()

    def __on_close(self, ws):
        """Called when the websocket is closed"""
        if self.status == "disconnected":
            return  # This can be double-called on disconnect
        logging.debug("ConnectorDB:WS: Websocket closed")

        # Turn off the ping timer
        if self.pingtimer is not None:
            self.pingtimer.cancel()

        self.disconnected_time = time.time()
        if self.status == "disconnecting":
            self.status = "disconnected"
        elif self.status == "connected":
            self.__reconnect()

    def __on_error(self, ws, err):
        """Called when there is an error in the websocket"""
        logging.debug("ConnectorDB:WS: Connection Error")

        if self.status == "connecting":
            self.status = "errored"
            self.ws_openlock.release()  # Release the lock of connecting

    def __on_message(self, ws, msg):
        """This function is called whenever there is a message received from the server"""
        msg = json.loads(msg)
        logging.debug("ConnectorDB:WS: Msg '%s'", msg["stream"])

        # Build the subcription key
        stream_key = msg["stream"] + ":"
        if "transform" in msg:
            stream_key += msg["transform"]

        self.subscription_lock.acquire()
        if stream_key in self.subscriptions:
            subscription_function = self.subscriptions[stream_key]
            self.subscription_lock.release()

            fresult = subscription_function(msg["stream"], msg["data"])

            if fresult is True:
                # This is a special result - if the subscription function of a downlink returns True,
                # then the datapoint is acknowledged automatically (ie, reinserted in non-downlink stream)
                fresult = msg["data"]

            if fresult is not False and fresult is not None and msg["stream"].endswith(
                    "/downlink") and msg["stream"].count("/") == 3:
                # If the above conditions are true, it means that the datapoints were from a downlink,
                # and the subscriber function chooses to acknowledge them, so we reinsert them.
                self.insert(msg["stream"][:-9], fresult)
        else:
            self.subscription_lock.release()
            logging.warn(
                "ConnectorDB:WS: Msg '%s' not subscribed! Subscriptions: %s",
                msg["stream"], list(self.subscriptions.keys()))

    def __on_ping(self, ws, data):
        """The server periodically sends us websocket ping messages to keep the connection alive. To
        ensure that the connection to the server is still active, we memorize the most recent ping's time
        and we periodically ensure that a ping was received in __ensure_ping"""
        logging.debug("ConnectorDB:WS: ping")
        self.lastpingtime = time.time()

    def __ensure_ping(self):
        """Each time the server sends a ping message, we record the timestamp. If we haven't received a ping
        within the given interval, then we assume that the connection was lost, close the websocket and
        attempt to reconnect"""

        logging.debug("ConnectorDB:WS: pingcheck")
        if (time.time() - self.lastpingtime > self.connection_ping_timeout):
            logging.warn("ConnectorDB:WS: Websocket ping timed out!")
            if self.ws is not None:
                self.ws.close()
                self.__on_close(self.ws)
        else:
            # reset the ping timer
            self.pingtimer = threading.Timer(self.connection_ping_timeout,
                                             self.__ensure_ping)
            self.pingtimer.daemon = True
            self.pingtimer.start()

    def __del__(self):
        """Make sure that all threads shut down when needed"""
        self.disconnect()
