#!/usr/local/bin/python3.5 -u

import threading
import socket
import sys
import time
import random



peerNumber = int(sys.argv[1])
successorOne = int(sys.argv[2])
successorTwo = int(sys.argv[3])

portNumber = peerNumber + 50000



portSuccessorOne = successorOne + 50000
portSuccessorTwo = successorTwo + 50000

global myPredecessors
global myPredecessorPorts

myPredecessors = [0, 0]
myPredecessorPorts = [0, 0]

mySuccessorsPorts = [portSuccessorOne, portSuccessorTwo]

def SendPing():

    global successorOne
    global successorTwo

    global portSuccessorOne
    global portSuccessorTwo

    host = 'localhost';

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        #print("SendPing socket has been created")
    except (socket.error):
        print('Failed to create socket')
        sys.exit()
     
    
    while(1) :

        print("\n\n\nmy successors are ", mySuccessorsPorts)
        pingRequestPort = mySuccessorsPorts[0]

        time.sleep(5)

        #msg = input('Enter message to send : ')
        
        msg = "PRQT:" + str(peerNumber)
        try:
            #Set the whole string
            s.sendto(msg.encode("utf-8"), (host, pingRequestPort))
            
            # receive data from client (data, addr)
            time.sleep(5)

            d = s.recvfrom(1024)
            reply = d[0]
            addr = d[1]

            if not reply:
                print("No reply was recieved!!")
                continue
            
            message = reply.strip().decode("utf-8")
            message = message.split(':')

            if message[0] == 'RSEP': 
                print("A ping response message was received from Peer " + message[1])
         
        except socket.error:
            print('Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
            sys.exit()

        pingRequestPort = mySuccessorsPorts[1]

        time.sleep(5)

        #msg = input('Enter message to send : ')
        
        msg = "PRQT:" + str(peerNumber)
        try:
            #Set the whole string
            s.sendto(msg.encode("utf-8"), (host, pingRequestPort))
            
            # receive data from client (data, addr)
            time.sleep(5)

            d = s.recvfrom(1024)
            reply = d[0]
            addr = d[1]

            if not reply:
                print("No reply was recieved!!")
                continue
            
            message = reply.strip().decode("utf-8")
            message = message.split(':')

            if message[0] == 'RSEP': 
                print("A ping response message was received from Peer " + message[1])
         
        except socket.error:
            print('Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
            sys.exit()

        time.sleep(50)




def AnswerPings():
    HOST = ''

    pingCount = 0
    global successorOne
    global successorTwo

    global portSuccessorOne
    global portSuccessorTwo
     
    # Datagram (udp) socket
    try :
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        #print("AnswerPings socket has been created on port ", portNumber)
    except (socket.error, msg):
        print('Failed to create socket. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
        sys.exit()
     
     
    # Bind socket to local host and port
    try:
        s.bind((HOST, portNumber))
    except(socket.error, msg):
        print('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
        sys.exit()
     
    #now keep talking with the client
    while 1:
        # receive data from client (data, addr)

        #time.sleep(3)

        d = s.recvfrom(1024)
        data = d[0]
        addr = d[1]
        
        if not data:
            #print("No ping request recieved yet.")
            continue
        
        message = data.strip().decode("utf-8")
        message = message.split(':')

        if message[0] == 'PRQT': 
            print("A ping request message was received from Peer " + message[1])

            pingCount = pingCount + 1

            if ((pingCount % 2) != 0):
                myPredecessors[0] = int(message[1])
                myPredecessorPorts[0] = int(message[1]) + 50000
            else:
                myPredecessors[1] = int(message[1])
                myPredecessorPorts[1] = int(message[1]) + 50000

            print("my Predecessors are now", myPredecessors)

            #reply = 'OK, AnswerPings recieved a request: ' + data.strip().decode("utf-8")
            reply = 'RSEP:' + str(peerNumber)
            s.sendto(reply.encode("utf-8") , addr)
         
    s.close()


def RequestFile():
    global successorOne
    global successorTwo

    global portSuccessorOne
    global portSuccessorTwo

    host = socket.gethostname();


    while(1):

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except (socket.error):
            print('Failed to create socket')
            sys.exit()

        s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        request = raw_input()
        #request = fileRequest.strip().decode("utf-8")
        request = request.strip().split(' ')

        if request[0] == 'quit':

            s3.connect((host, myPredecessorPorts[0]))
            msg = 'QUITp1:' + str(peerNumber) + ":" + str(successorOne) + ":" + str(successorTwo)
            s3.sendall(msg)
            #s.close()

            s2.connect((host, myPredecessorPorts[1]))
            msg = 'QUITp2:' + str(peerNumber) + ":" + str(successorOne)
            s2.sendall(msg)
            #s.close()


        elif request[0] == 'request':
            

            try:
                s.connect((host, portSuccessorOne))
            except(socket.error):
                print('Connect failed. At successorOne socket: ', portSuccessorOne)
                sys.exit()
            print('Connected to my first successor socket: ', portSuccessorOne)


            file = request[1]
            fileHash = int(request[1]) % 256
            msg = 'FRQT:' + file + ":" + str(peerNumber)

            try:
                #Set the whole string
                s.sendall(msg) #.encode("utf-8"))
                print("File request message for ", file, " with hash code ", fileHash, " has been sent to my successor.")

                
                # receive data from client (data, addr)
                #time.sleep(5)

                data = s.recv(1024)

                if not data:
                    #print("No ping request recieved yet.")
                    continue
            
                message = data.decode("utf-8")
                message = message.split(':')

                print("Recieved this message:", message)

                if message[0] == 'FRESP':
                    continue
                #reply = d[0]
                #addr = d[1]

                #if not reply:
                #    print("No reply was recieved!!")
                #    break
                
                #message = reply.strip().decode("utf-8")
                #message = message.split(':')

                #if message[0] == 'RSEP': 
                    #print("A ping response message was received from Peer " + message[1])
            except(socket.error):
                print('Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
                sys.exit()

        s.close()


def SendFile():
    global successorOne
    global successorTwo

    global portSuccessorOne
    global portSuccessorTwo

    HOST = ''
    hostName = socket.gethostname();
     
    # Datagram (udp) socket
    try :
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #print("AnswerPings socket has been created on port ", portNumber)
    except (socket.error, msg):
        print('Failed to create socket. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
        sys.exit()
    
    try :
        s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #print("AnswerPings socket has been created on port ", portNumber)
    except (socket.error, msg):
        print('Failed to create socket s2. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
        sys.exit()
     
    # Bind socket to local host and port
    try:
        s.bind((HOST, portNumber))
    except(socket.error):
        print('Bind failed!!!')
        # Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
        sys.exit()
    
    s.listen(1)
    
    #now keep talking with the client
    while 1:
        conn, addr = s.accept()
        #print("connection accepted**")
        while 1:
            
            # receive data from client (data, addr)

            #time.sleep(3)

            try:
                data = conn.recv(1024)
                #print(d, "\n\n\n\n")
                #data = d[0]
                #addr = d[1]
            
                if not data:
                    #print("No ping request recieved yet.")
                    continue
            
                message = data #.decode("utf-8")
                message = message.split(':')
                #print("Recieved this message:", message)


                fileRequestType = message[0]

                if fileRequestType == 'QUITp1':
                    print("Peer ", message[1], " will depart from the network.")
                    successorOne = int(message[2])
                    portSuccessorOne = successorOne + 50000
                    successorTwo = int(message[3])
                    portSuccessorTwo = successorTwo + 50000
                    print("My first successor is now peer ", message[2])
                    print("My second successor is now peer ", message[3])

                if fileRequestType == 'QUITp2':
                    print("Peer ", message[1], " will depart from the network.")
                    print("My first successor is now peer ", successorOne)
                    print("My second successor is now peer ", message[2])
                    successorTwo = int(message[2])
                    portSuccessorTwo = successorTwo + 50000




                file = int(message[1])
                fileHash = (file % 256)

                peerRequestingFile = int(message[2])
                portRequestingFile = peerRequestingFile + 50000

                if fileRequestType == 'FRESP':
                    print("Received a response message from peer ", str(message[2]), ", which has the file ", str(file))

                elif fileRequestType == 'FRQT':

                    if (fileHash <= peerNumber or peerRequestingFile > peerNumber) and (fileHash > peerRequestingFile):

                        #print(fileHash, " is stored here.")
                        print("File ", file, "is here.")
                        msg = "FRESP:" + str(file) + ":" + str(peerNumber)
                        
                        conn.sendall(msg) #.encode("utf-8"))
                        #print("print 1. just sent the following: ", msg)

                        print("A response message, destined for peer ", peerRequestingFile, ", has been sent.")

                    else:
                        #print(fileHash, peerNumber, peerRequestingFile, peerNumber, fileHash, peerRequestingFile)
                        print("File ", file, " with hash ", fileHash, " is not stored here")
                        msg = 'FRQTF:' + str(file) + ":" + str(peerRequestingFile) + ":" + str(peerNumber)

                        try:
                            s2.connect((hostName, portSuccessorOne))
                        except(socket.error):
                            print('Connect failed. At successorOne socket: ', portSuccessorOne)
                            sys.exit()
                        #s2.connect((hostName, portSuccessorOne))
                        s2.sendall(msg)
                        #conn.sendall(msg)

                        print("File request message for ", file, " has been forwarded to my successor.")
                        #print("it was sent to port number", portSuccessorOne)     



                elif fileRequestType == 'FRQTF':
                    previousPeer = int(message[3])
                    #print(fileHash, peerNumber, previousPeer, peerNumber, fileHash, previousPeer)
                    if (fileHash <= peerNumber or previousPeer > peerNumber) and (fileHash > previousPeer):

                        #print(fileHash, " is stored here.")
                        print("File ", file, "is here.")
                        msg = "FRESP:" + str(file) + ":" + str(peerNumber)
                        s2.connect((hostName, portRequestingFile))
                        s2.sendall(msg) #.encode("utf-8"))
                        #print("print 2. just sent the following: ", msg)

                        print("A response message, destined for peer ", peerRequestingFile, ", has been sent.")

                    else:
                        
                        #print(fileHash, peerNumber, peerRequestingFile, peerNumber, fileHash, peerRequestingFile)
                        print("File ", file, " with hash ", fileHash, " is not stored here")
                        msg = 'FRQTF:' + str(file) + ":" + str(peerRequestingFile) + ":" + str(peerNumber)

                        s2.connect((hostName, portSuccessorOne))
                        s2.sendall(msg)


                        print("File request message for ", file, " has been forwarded to my successor.")

                    #reply = 'OK, AnswerPings recieved a request: ' + data.strip().decode("utf-8")
                    #reply = 'RSEP:' + str(peerNumber)
                    #s.sendto(reply.encode("utf-8") , addr)
            except socket.error:
                print("Error Occured.")
                break

        conn.close()
        #print("Connection closed")







thread1 = threading.Thread(target=AnswerPings)
thread1.start()

thread2 = threading.Thread(target=SendPing)
thread2.start()

thread4 = threading.Thread(target=SendFile)
thread4.start()

time.sleep(7)

thread3 = threading.Thread(target=RequestFile)
thread3.start()



