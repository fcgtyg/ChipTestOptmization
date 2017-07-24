import socket
from time import sleep
from random import randint

def start():
    host = 'localhost'
    port = 12344
    address = (host, port)
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(address)
    server_socket.listen(5)
    print "Listening for client . . ."
    conn, address = server_socket.accept()
    print "Connected to trainer client at", address, "\n"
    counter = 0
    while counter<=300:
        try:
            output = str(randint(1, 10))+"\n"
            conn.send(output)
            counter+=1
            sleep(0.1)
        except:
            print "Disconnected from", address
            break
    conn.close()
    print "Training Data Completed.\n"
    return 0