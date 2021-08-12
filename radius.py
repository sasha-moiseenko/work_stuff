import redis
import time
from pyrad import dictionary, packet, server
import logging
import socket

logger = logging.getLogger('RADIUS')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(handler)


class DpiNotifier:
    @staticmethod
    def SessionManage(action, pkt, action_description):
        FramedIPAddress = pkt['Framed-IP-Address'] if 'Framed-IP-Address' in pkt.keys() else ''
        UserName = pkt['User-Name'] if 'Framed-IP-Address' in pkt.keys() else ''
        connect_redis = redis.Redis(
            host='localhost',
            port='49153')
        if FramedIPAddress and UserName:
            logger.info('+ {} gu{} => {}'.format(action_description, FramedIPAddress, UserName))
            message = '{};{};{}/32;{};'.format(
                action,
                UserName[0],
                FramedIPAddress[0],
                '')
            connect_redis.set(UserName[0], FramedIPAddress[0])
            DpiNotifier.send_via_socket(message)
        pass

    @staticmethod
    def send_via_socket(message):
        ce_ip = '127.0.0.1'
        ce_port = 33333
        try:
            socket.socket(socket.AF_INET, socket.SOCK_DGRAM).sendto(
                message.encode(), (ce_ip, ce_port))
        except Exception as err:
            print('CEUpdater: %s', err)


class FakeServer(server.Server):
    def HandleAuthPacket(self, pkt):
        # print("Received an authentication request")
        # print("Attributes: ")
        # for attr in pkt.keys():
        # 	print("\t%s: %s" % (attr, pkt[attr]))
        DpiNotifier.SessionManage('+', pkt, 'start')

    def HandleAcctPacket(self, pkt):
        # print("Received an accounting request")
        # print("Attributes: ")
        # for attr in pkt.keys():
        # 	print("\t%s: %s" % (attr, pkt[attr]))
        DpiNotifier.SessionManage('+', pkt, 'acct')

    def HandleDisconnectPacket(self, pkt):
        # print("Received an disconnect request")
        # print("Attributes: ")
        # for attr in pkt.keys():
        # 	print("\t%s: %s" % (attr, pkt[attr]))
        DpiNotifier.SessionManage('-', pkt, 'stop')


if __name__ == '__main__':
    # create server and read dictionary
    srv = FakeServer(dict=dictionary.Dictionary("/opt/projects/a1-radius-collector/radius-collector/dictionary"), coa_enabled=True)

    # add clients (address, secret, name)
    srv.hosts["0.0.0.0"] = server.RemoteHost("0.0.0.0", b"Kah3choteereethiejeimaeziecumi", "localhost")
    srv.BindToAddress("0.0.0.0")
    logger.info('Starting...')
    # start server
    srv.Run()

# def redis_maintain():
#     connect_redis = redis.Redis(
#         host='localhost',
#         port='49153')
#
#     connect_redis.set('mykey', 'Hello from Python!')
#     value = connect_redis.get('mykey')
#     print(value)
#
#     connect_redis.zadd('vehicles', {'car': 0})
#     connect_redis.zadd('vehicles', {'bike': 0})
#     vehicles = connect_redis.zrange('vehicles', 0, -1)
#     """
#          Return a range of values from sorted set ``name`` between
#          ``start`` and ``end`` sorted in ascending order.
#          ``start`` and ``end`` can be negative, indicating the end of the range.
#          ``desc`` a boolean indicating whether to sort the results descendingly
#          ``withscores`` indicates to return the scores along with the values.
#          The return type is a list of (value, score) pairs
#          ``score_cast_func`` a callable used to cast the score return value
#          """
#     print(vehicles)
#
#
# redis_maintain()
