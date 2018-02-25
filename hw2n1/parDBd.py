# Importing all the necessary libraries.
import socket

import sqlite3
from sqlite3 import Error

import sys
from sys import argv

# A main function that takes in two commandline arguments.
def Main(argv):
  host = argv[1];
  port = argv[2];

  # Store data received into datas array.
  datas = [];

  mySocket = socket.socket()
  mySocket.bind((str(host),int(port)))

  while(1):
    mySocket.listen(1)
    conn, addr = mySocket.accept()
    # print ("Server: Connection from " + str(addr))
    data = conn.recv(1024).decode()
    # print("DATA:", data);
    if not data:
      return
    # print ("Server: recv " + str(data));
    datas.append(data.split("$")[0]);
    datas.append(data.split("$")[1]);

    # Connect to sqlite database in node1 directory and execute DDL command.
    try:
      condb = sqlite3.connect(datas[0].strip('/'));
      cur = condb.cursor();
      cur.execute(datas[1]);
      message = "./books.sql success.$" + host + ":" +  str(port) + "$" + datas[0] + "$" + str(cur.fetchall());
      condb.commit();
      conn.send(message.encode());
    # If there is an error, send a message back to client that it was a failure.
    except Error as e:
      print(e);
      message = "./books.sql failure.$" + host + ":" + str(port) + "$" + datas[0];
      conn.send(message.encode());
      conn.close();
    # After everything, finally close the db and the connection between client / server.
    finally:
      conn.close();
# Run main function with argv parameters (commandline arguments)
Main(argv);
