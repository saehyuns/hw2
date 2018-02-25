import re

import socket

import sys
from sys import argv

import sqlite3
from sqlite3 import Error

import multiprocessing

def runSQL(argv):
  url = '';
  hostname = '';
  port = '';
  db = '';
  numnodes = 0;
  nodes = [];
  tablename = argv[2];
  tablename = tablename.replace(".sql", "").upper();
  message = "";

  data = [];
  ddlCommands = [];

  if argv[1] == "cluster.cfg":
    configFile = open(argv[1], "r");
    data = configFile.read().strip().replace("\n", ";").split(";");
    data = list(filter(('').__ne__, data));
    configFile.close();
  else:
    print("Please enter 'cluster.cfg' as the first commandline argument!");
  if '.sql' in argv[2]:
    ddlFile = open(argv[2], "r");
    ddlCommands = ddlFile.read().strip().replace("\n", ";").split(";");
    ddlCommands = list(filter(('').__ne__, ddlCommands));
    ddlFile.close();
  else:
    print("Please enter '[Insert Table Name].sql' as the first commandline argument!");

  for d in data:
    if d.strip():
      temp = d.strip().split("=");
      if temp[0].find("catalog") > -1:
        if temp[0].find("hostname") > -1:
          url = temp[1];
          hostname = temp[1].split("/")[0].split(":")[0];
          port = temp[1].split("/")[0].split(":")[1];
          db = temp[1].split("/")[1];
          nodes.append(Node(url, hostname, port, db));
      if temp[0].find("node") > -1:
        if temp[0].find("hostname") > -1:
          url = temp[1];
          hostname = temp[1].split("/")[0].split(":")[0];
          port = temp[1].split("/")[0].split(":")[1];
          db = temp[1].split("/")[1];
          nodes.append(Node(url, hostname, port, db));

  numnodes = len(nodes);
  
  p1 = multiprocessing.Process(target=connect, args=(nodes[0], nodes[1], ddlCommands, tablename,));  
  p2 = multiprocessing.Process(target=connect, args=(nodes[0], nodes[2], ddlCommands, tablename,));  
  
  p1.start();
  p2.start();

  p1.join();
  p2.join();
  
  print("DONE");

def connect(catalogNode, serverNode, commands, tablename):
  # URL: 172.17.0.2:5000/mydb1 HOSTNAME: 172.17.0.2 PORT: 5000 DB: mydb1
  # node.displayNode();
  url = serverNode.url;
  hostname = serverNode.hostname;
  port = serverNode.port;
  database = serverNode.db;
  
  for i in range(0, len(commands)):
    message = database + "$" + commands[i];

    mySocket = socket.socket();
    mySocket.connect((str(hostname),int(port)));
    mySocket.send(message.encode());

    received = mySocket.recv(1024).decode();
    receivedp = received.split("$");
    receivedp[3] = re.sub("'", '', receivedp[3]);
    
    print(receivedp[3]);
  
    print("[" + url + "]: " + receivedp[0]);
    if(receivedp[0] == "./books.sql success."):
      message = receivedp[0] + "$" + tablename + "$" + url;
      mySocket = socket.socket();
      mySocket.connect((catalogNode.hostname, int(catalogNode.port)));
      mySocket.send(message.encode());
      received = mySocket.recv(1024).decode();
      print("[" + catalogNode.url + "]: " + received);

  mySocket.close();
  

# A class called node containing the url, hostname, port, and db name of the node.
class Node:
  def __init__(self, url, hostname, port, db):
    self.url = url;
    self.hostname = hostname;
    self.port = port;
    self.db = db;
  def displayNode(self):
    print("URL:", self.url, "HOSTNAME:", self.hostname, "PORT:", self.port, "DB:", self.db);
 
if __name__ == '__main__':
  runSQL(argv);
