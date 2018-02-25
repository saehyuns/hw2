# Import all necessary libraries. 
import socket

import sqlite3
from sqlite3 import Error

import sys
from sys import argv

# A Main function which listens for a message from the nodes to create catalog db and update the db.
def Main(argv):
  host = argv[1];
  port = argv[2];

  # Messages received will be store in datas array.
  datas = [];

  mySocket = socket.socket()
  mySocket.bind((str(host),int(port)))

  while(1):
    mySocket.listen(1)
    conn, addr = mySocket.accept()
    # print ("Server: Connection from " + str(addr))
    data = conn.recv(1024).decode()
    if not data:
        return
    # print ("Server: recv " + str(data));
    datap = data.split("$");
    id = 0;
    if datap[2].find("mydb1") > -1:
      id = 1;
      # Connect to the mycatdb sqlite3 database and execute a create table / insert DDL command.
      try:
        con = sqlite3.connect("mycatdb");
        cur = con.cursor();
        cur.execute("CREATE TABLE IF NOT EXISTS DTABLES(tname char(32), nodedriver char(64), nodeurl char(128), nodeuser char(16), nodepasswd char(16), partmtd int, nodeid int, partcol char(32), partparam1 char(32), partparam2 char(32));");
        if(len(datap) >= 9):
          cur.execute("INSERT INTO DTABLES(tname, nodedriver, nodeurl, nodeuser, nodepasswd, partmtd, nodeid, partcol, partparam1, partparam2) VALUES (" + "'" + datap[0] + "'" + ", " + "'" + datap[1] + "'" + ", " + "'" + datap[2] + "'" + ", " + "'" + datap[3] + "'" + ", " + "'" + datap[4] + "'" + ", " + "'" + datap[5] + "'" + ", " + "'" + str(id) + "'" + ", " + "'" + datap[6] + "'" + ", " + "'" + datap[7] + "'" + ", " + "'" + data[8] + "');");
        else:
          cur.execute("INSERT INTO DTABLES(tname, nodedriver, nodeurl, nodeuser, nodepasswd, partmtd, nodeid, partcol, partparam1, partparam2) VALUES (" + "'" + datap[1] + "'" + ", NULL, " + "'" + datap[2] + "'" + ", NULL, NULL, NULL, " + "'" + str(id) + "'" + ", NULL, NULL, NULL);");
        con.commit();
        message = "catalog updated.";
        conn.send(message.encode()); 
      # If there is an error send back an error message to client.
      except Error as e:
        print(e);
        message = e;
        conn.send(message.encode()); 
        con.close(); 
      # Finally, close all connections.
      finally:
        con.close(); 
    elif datap[2].find("mydb2") > -1:
      id = 2;
      # Connect to the mycatdb sqlite3 database and execute a create table / insert DDL command.
      try:
        con = sqlite3.connect("mycatdb");
        cur = con.cursor();
        cur.execute("CREATE TABLE IF NOT EXISTS DTABLES(tname char(32), nodedriver char(64), nodeurl char(128), nodeuser char(16), nodepasswd char(16), partmtd int, nodeid int, partcol char(32), partparam1 char(32), partparam2 char(32));");
        if(len(datap) >= 9):
          cur.execute("INSERT INTO DTABLES(tname, nodedriver, nodeurl, nodeuser, nodepasswd, partmtd, nodeid, partcol, partparam1, partparam2) VALUES (" + "'" + datap[0] + "'" + ", " + "'" + datap[1] + "'" + ", " + "'" + datap[2] + "'" + ", " + "'" + datap[3] + "'" + ", " + "'" + datap[4] + "'" + ", " + "'" + datap[5] + "'" + ", " + "'" + str(id) + "'" + ", " + "'" + datap[6] + "'" + ", " + "'" + datap[7] + "'" + ", " + "'" + data[8] + "');");
        else:
          cur.execute("INSERT INTO DTABLES(tname, nodedriver, nodeurl, nodeuser, nodepasswd, partmtd, nodeid, partcol, partparam1, partparam2) VALUES (" + "'" + datap[1] + "'" + ", NULL, " + "'" + datap[2] + "'" + ", NULL, NULL, NULL, " + "'" + str(id) + "'" + ", NULL, NULL, NULL);");
        con.commit();
        message = "catalog updated.";
        conn.send(message.encode()); 
      # If there is an error send back an error message to client.
      except Error as e:
        print(e);
        message = e;
        conn.send(message.encode()); 
        con.close(); 
      # Finally, close all connections.
      finally:
        con.close(); 
    else:
      nodes = [];
      results = [];
      database = '';

      datap = data.split("$");
      database = datap[0];
      for i in range(1, len(datap)):
        nodes.append(datap[i]);
      try:
        con = sqlite3.connect(database);
        cur = con.cursor();
        for node in nodes:
          cur.execute("SELECT DISTINCT NODEURL FROM DTABLES WHERE NODEID=" + node);
          results.append(cur.fetchone());
          con.commit();
        message = "catalog updated.";
      # If there is an error send back an error message to client.
      except Error as e:
        print(e);
        message = e;
        conn.send(message.encode());
        con.close();
      # Finally, close all connections.
      finally:
        con.close();
      
      for result in results:
        message += "$" + str(result);
      conn.send(message.encode());

Main(argv);
