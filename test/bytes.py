from lib.socket.package import *

# print((2).to_bytes(32, byteorder='big'))
# exit()

header = b'' + (11).to_bytes(32, byteorder='big') + (1).to_bytes(8, byteorder='big') + (2).to_bytes(8, byteorder='big')

print('hallo'.encode('utf-8'))

print(header)

size, type, id = read_header(header)

print(size, type, id)
