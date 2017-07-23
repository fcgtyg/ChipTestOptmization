import socket
from random import randint
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect(("localhost", 8000))


while True:
    came = client_socket.recv(1024)
    client_socket.send(came)
    if (int(came) > 10):
        print came
