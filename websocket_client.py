import logging
import pysher

#
# logger = logging.getLogger('websockets')
# logger.setLevel(logging.INFO)
# logger.addHandler(logging.StreamHandler())
#
#
# async def client_connection(uri):
#     async with connect(uri) as websocket:
#         await websocket.send("channels")
#         await websocket.recv()
#
#
# asyncio.run(client_connection("ws://192.168.200.11:7001/app/app/test_channel"))
#
# def my_func(*args, **kwargs):
#     print('processing Args: ', args)
#     print('processing Kwargs: ', kwargs)


# private
pusher = pysher.Pusher(key="key", custom_host="labns.navekscreen.video")


def my_func(*args, **kwargs):
    print("Processing Args:", args)
    print("Processing Kwargs", kwargs)


def connect_handler(data):
    print("Data: ", data)
    private_channel = pusher.subscribe("private-user.",
                                       authEndpoint='https://labns.navekscreen.video/api/v3/broadcasting/auth',
                                       url_token="https://labns.navekscreen.video/api/v3/token",
                                       payload={'login': login, 'password': passw})
    private_channel.bind('App\\Events\\UserPush', my_func())


pusher.connection.bind('pusher:connection_established', connect_handler)
pusher.connect()

# def generate_auth_token(self,channel_name, authEndpoint,url_token, payload):
#     self.auth_token = get_access_token(url=url_token, payload=payload)
#     print('auth_token:  ' + self.auth_token)
#     headers = {'Accept': 'application/json', 'Authorization': self.auth_token}
#     channel_name = f'{channel_name}{CHANNEL_ID}'
#     socket_id = self.connection.socket_id
#     form_data = {'socket_id': socket_id, 'channel_name': channel_name}
#
#     auth_response = requests.post(url=authEndpoint, headers=headers, json=form_data)
#     auth_key_data = auth_response.json()
#     auth_key = auth_key_data['auth']
#     print(auth_key)
#     return auth_key
