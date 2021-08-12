__docformat__ = "epytext en"

import hashlib
import select
import socket
import time
import six
import struct
from pyrad import host
from pyrad import packet


class Client(host.Host):
    def __init__(self, server, authport=1812, acctport=1813,
                 coaport=3799, secret=six.b(''), dict=None, retries=3, timeout=5):
        host.Host.__init__(self, authport, acctport, coaport, dict)

        self.server = server
        self.secret = secret
        self._socket = None
        self.retries = retries
        self.timeout = timeout
        self._poll = select.poll()

    def CreateAcctPacket(self, **args):
        """Create a new RADIUS packet.
        This utility function creates a new RADIUS packet which can
        be used to communicate with the RADIUS server this client
        talks to. This is initializing the new packet with the
        dictionary and secret used for the client.
        :return: a new empty packet instance
        :rtype:  pyrad.packet.Packet
        """
        return host.Host.CreateAcctPacket(self, secret=self.secret, **args)
