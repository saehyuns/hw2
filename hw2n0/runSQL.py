# Import necessary libraries / packages
import re

import socket

import sys
from sys import argv

import sqlite3
from sqlite3 import Error

import multiprocessing

# Function called runSQL takes in two commandline arguments
def runSQL(argv):
  # Initialize variables
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

  # Read int he cluster.cfg
  if argv[1] == "cluster.cfg":
    configFile = open(argv[1], "r");
    data = configFile.read().strip().replace("\n", ";").split(";");
    data = list(filter(('').__ne__, data));
    configFile.close();
  # Print error messag eif not cluster.cfg
  else:
    print("Please enter 'cluster.cfg' as the first commandline argument!");
  # Read in an .sql file
  if '.sql' in argv[2]:
    ddlFile = open(argv[2], "r");
    ddlCommands = ddlFile.read().strip().replace("\n", ";").split(";");
    ddlCommands = list(filter(('').__ne__, ddlCommands));
    ddlFile.close();
  # Print Error is not an .sql file.
  else:
    print("Please enter '[Insert Table Name].sql' as the first commandline argument!");

  # Parse the data and store into variabels
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
  
  # Multiprocessing / threading portion
  p1 = multiprocessing.Process(target=connect, args=(nodes[0], nodes[1], ddlCommands, tablename,));  
  p2 = multiprocessing.Process(target=connect, args=(nodes[0], nodes[2], ddlCommands, tablename,));  
  
  # Start the multi threading process
  p1.start();
  p2.start();

  # "Join" the two threads
  p1.join();
  p2.join();
  
  # Print done when both processes are done
  print("DONE");

# Function called connec that performs DDL on given servers via threading
def connect(catalogNode, serverNode, commands, tablename):
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
