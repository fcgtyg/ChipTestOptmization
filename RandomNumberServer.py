import socket
from time import sleep
from random import randint

def start():
    host = 'localhost'
    port = 12345
    address = (host, port)
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(address)
    server_socket.listen(5)
    while True:
        print "Listening for client . . ."
        conn, address = server_socket.accept()
        print "Connected to client at", address
        #pick a large output buffer size because i dont necessarily know how big the incoming packet is
        while True:
            try:
                output = str(randint(1, 10))+"\n"
                conn.send(output)
                sleep(0.1)
            except:
                print "Disconnected from", address
                break
