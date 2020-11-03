import asyncio
import json

from lib.socket.package import *

connected_listeners = []
connected_workers = []

series = ('hub_test1', 'hub_test2')


async def handle_echo(reader, writer):

    connected = True

    while connected:
        packet_type, packet_id, data = await read_packet(reader)

        addr = writer.get_extra_info('peername')
        print("Received %r from %r" % (packet_id, addr))
        if packet_id == 0:
            connected = False

        if packet_type == HANDSHAKE_LISTENER:
            client_id = data.decode("utf-8")
            connected_listeners.append(client_id)
            print(f'New listener with id: {client_id}')
            response = create_header(0, HANDSHAKE_OK, packet_id)
            writer.write(response)

            update = json.dumps(series)
            series_update = create_header(len(update), UPDATE_SERIES, packet_id)
            writer.write(series_update + update.encode("utf-8"))

        if packet_type == HANDSHAKE_WORKER:
            client_id = data.decode("utf-8")
            connected_workers.append(client_id)
            print(f'New worker with id: {client_id}')
            response = create_header(0, HANDSHAKE_OK, packet_id)
            writer.write(response)

        if packet_type == HEARTBEAT:
            client_id = data.decode("utf-8")
            print(f'Heartbeat from worker/listener with id: {client_id}')
            response = create_header(0, HEARTBEAT, packet_id)
            writer.write(response)

        if packet_type == LISTENER_ADD_SERIE_COUNT:
            data = json.loads(data.decode("utf-8"))
            print(f'Update from listener with id: {client_id}')
            print(data)
            response = create_header(0, REPONSE_OK, packet_id)
            writer.write(response)

        await writer.drain()

    print("Close the client socket")
    writer.close()




loop = asyncio.get_event_loop()
coro = asyncio.start_server(handle_echo, '127.0.0.1', 9103, loop=loop)
server = loop.run_until_complete(coro)

# Serve requests until Ctrl+C is pressed
print('Serving on {}'.format(server.sockets[0].getsockname()))
try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

# Close the server
server.close()
loop.run_until_complete(server.wait_closed())
loop.close()
