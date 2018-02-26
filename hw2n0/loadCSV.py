# Import needed packages
import socket

import re

import sys
from sys import argv

import sqlite3
from sqlite3 import Error

import multiprocessing

import csv

# Load function that takes commandline arguments
def load(argv):
  # Initialize variables
  tname = '';
  nodedriver = ''; 
  nodeurl = '';
  nodeuser = '';
  nodepasswd = '';
  partmtd = '';
  nodeid = '';
  partcol = '';
  partparam1 = '';
  partparam2 = '';
 
  configData = [];
  csvData = [];
  tables = [];

  parType = '';
  
  # If the config file is of type range partition run this part of the code
  if (argv[1] == "range.cfg"):
    parType = "range";

    # Open the config file for reading
    configFile = open(argv[1], "r");
    configData = configFile.read().strip().replace("\n", ";").split(";");
    configData = list(filter(('').__ne__, configData));
    configFile.close();

    # Parse through the config file and assign them into variables
    for d in configData:
      if d.strip():
        temp = d.strip().split("=");
        if temp[0].find("catalog") > -1:
          if temp[0].find("driver") > -1:
            nodedriver = temp[1];
          if temp[0].find("hostname") > -1:
            nodeurl = temp[1];
          if temp[0].find("username") > -1:
            nodeuser = temp[1];
          if temp[0].find("passwd") > - 1:
            nodepasswd = temp[1];
        if temp[0].find("tablename") > -1:
          tname = temp[1];
        if temp[0].find("partition") > -1:
          if temp[0].find("method") > -1:
            partmtd = 1;
          if temp[0].find("column") > -1:
            partcol = temp[1];
          if temp[0].find("node1") > -1:
            nodeid = 1;
            if temp[0].find("param1") > -1:
              partparam1 = temp[1];
            if temp[0].find("param2") > -1:
              partparam2 = temp[1];
              tables.append(Table(tname, nodedriver, nodeurl, nodeuser, nodepasswd, partmtd, nodeid, partcol, partparam1, partparam2));
          if temp[0].find("node2") > -1:
            nodeid = 2;
            if temp[0].find("param1") > -1:
              partparam1 = temp[1];
            if temp[0].find("param2") > -1:
              partparam2 = temp[1];
              tables.append(Table(tname, nodedriver, nodeurl, nodeuser, nodepasswd, partmtd, nodeid, partcol, partparam1, partparam2));

  # If the config file is of type hash partition run this part of the code
  elif argv[1] == "hash.cfg":
    parType = "hash";
   
    # Open the config file to reading
    configFile = open(argv[1], "r");
    configData = configFile.read().strip().replace("\n", ";").split(";");
    configData = list(filter(('').__ne__, configData));
    configFile.close();
   
    # Parse through the data and store them into variables
    for d in configData:
      if d.strip():
        temp = d.strip().split("=");
        if temp[0].find("catalog") > -1:
          if temp[0].find("driver") > -1:
            nodedriver = temp[1];
          if temp[0].find("hostname") > -1:
            nodeurl = temp[1];
          if temp[0].find("username") > -1:
            nodeuser = temp[1];
          if temp[0].find("passwd") > - 1:
            nodepasswd = temp[1];
        if temp[0].find("tablename") > -1:
          tname = temp[1];
        if temp[0].find("partition") > -1:
          if temp[0].find("method") > -1:
            partmtd = 2;
          if temp[0].find("column") > -1:
            partcol = temp[1];
          if temp[0].find("param1") > -1:
            partparam1 = temp[1];
    tables.append(Table(tname, nodedriver, nodeurl, nodeuser, nodepasswd, partmtd, nodeid, partcol, partparam1, partparam2));
  # If it is neither of the config file, print error message
  else:
    print("The config file has to be 'hash.cfg' or 'range.cfg'!");
    parType = "none";
  
  # Read from books.csv
  if (argv[2] == "books.csv"):
    csvFile = open(argv[2], "r");
    csvReader = csv.reader(csvFile, delimiter=",")
    for row in csvReader:
      csvData.append(row);
  # If it is not books.csv, print error message
  else:
    print("Please enter 'books.csv' as the second commandline argument!");
  
  temp = tables[0].nodeurl.split("/");
  database = temp[1];

  temp = temp[0].split(":");
  hostname = temp[0];
  port = temp[1];

  # If partition type is hash run this part of the code
  if parType == "hash":
    nodes =  [];
    for i in range (0, len(csvData)):
      tables[0].nodeid = (int(csvData[i][0]) % int(tables[0].partparam1)) + 1;
      nodes.append(tables[0].nodeid);
   
    message = database;
    for node in nodes:
      message += "$" + str(node);
    
    mySocket = socket.socket();
    mySocket.connect((str(hostname), int(port)));
    mySocket.send(message.encode()); 
    
    received = mySocket.recv(1024).decode();
    receivedp = received.split("$");
    result = receivedp[0];

    dbNodes = [];
    for i in range(1, len(receivedp)):
      dbNodes.append(receivedp[i]);

    hostnames = [];
    ports = [];
    databases = [];

    # Remove unnecessary symbols
    for i in range(0, len(dbNodes)):
      count = nodes.count(i+1);
      dbNodes[i] = re.sub("[)',(]", '', dbNodes[i]);
      databases.append(dbNodes[i].split("/")[1]);
      hostnames.append(dbNodes[i].split("/")[0].split(":")[0]);
      ports.append(dbNodes[i].split("/")[0].split(":")[1]);

      mySocket = socket.socket();
      mySocket.connect((str(hostnames[i]), int(ports[i])));
      message = databases[i];

      command = "insert into books (isbn, title, price) values (" + "'" + csvData[i][0] + "', " + "'" + csvData[i][1] + "', " + "'" + csvData[i][2] + "');";

      message += "$" + command;
      mySocket.send(message.encode());
      received = mySocket.recv(1024).decode();

      receivedp = received.split("$");

      print("[" + dbNodes[i] + "]: " + str(count) + " rows inserted.");
      message = str(tables[0].tname) + "$" + str(tables[0].nodedriver) + "$" + dbNodes[i] + "$" + str(tables[0].nodeuser) + "$" + str(tables[0].nodepasswd) + "$" + str(tables[0].partmtd) + "$" + str(tables[0].partcol) + "$" + str(tables[0].partparam1) + "$" + str(tables[0].partparam2);
      mySocket = socket.socket();
      mySocket.connect((str(hostname), int(port)));
      mySocket.send(message.encode());
      received = mySocket.recv(1024).decode();
      print("[" + tables[0].nodeurl + "]: " + received); 

    mySocket.close();
    
  # If the partition type is range, run this part of the code
  elif parType == "range":
    nodes =  [];
    for i in range (0, len(csvData)):
      if (int(csvData[i][0]) > int(tables[0].partparam1)) and (int(csvData[i][0]) <= int(tables[0].partparam2)):
        tables[0].nodeid = 1;
        nodes.append(tables[0].nodeid);
      elif (int(csvData[i][0]) > int(tables[1].partparam1)) and (int(csvData[i][0]) <= int(tables[1].partparam2)):
        tables[1].nodeid = 2;
        nodes.append(tables[1].nodeid);

    message = database;
    for node in nodes:
      message += "$" + str(node);

    mySocket = socket.socket();
    mySocket.connect((str(hostname), int(port)));
    mySocket.send(message.encode());

    received = mySocket.recv(1024).decode();
    receivedp = received.split("$");
    result = receivedp[0];
    
    dbNodes = [];
    for i in range(1, len(receivedp)):
      dbNodes.append(receivedp[i]);

    hostnames = [];
    ports = [];
    databases = [];

    # Remove unnecessary symbols
    for i in range(0, len(dbNodes)):
      count = nodes.count(i+1);
      dbNodes[i] = re.sub("[)',(]", '', dbNodes[i]);
      databases.append(dbNodes[i].split("/")[1]);
      hostnames.append(dbNodes[i].split("/")[0].split(":")[0]);
      ports.append(dbNodes[i].split("/")[0].split(":")[1]);

      mySocket = socket.socket();
      mySocket.connect((str(hostnames[i]), int(ports[i])));
      message = databases[i];

      command = "insert into books (isbn, title, price) values (" + "'" + csvData[i][0] + "', " + "'" + csvData[i][1] + "', " + "'" + csvData[i][2] + "');";

      message += "$" + command;
      mySocket.send(message.encode());
      received = mySocket.recv(1024).decode();

      receivedp = received.split("$");

      print("[" + dbNodes[i] + "]: " + str(count) + " rows inserted.");
      message = str(tables[0].tname) + "$" + str(tables[0].nodedriver) + "$" + dbNodes[i] + "$" + str(tables[0].nodeuser) + "$" + str(tables[0].nodepasswd) + "$" + str(tables[0].partmtd) + "$" + str(tables[0].partcol) + "$" + str(tables[0].partparam1) + "$" + str(tables[0].partparam2);
      mySocket = socket.socket();
      mySocket.connect((str(hostname), int(port)));
      mySocket.send(message.encode());
      received = mySocket.recv(1024).decode();
      print("[" + tables[0].nodeurl + "]: " + received);

    mySocket.close();

  else:
    print("DOING NO PARTITION...\n");
  
 
class Table:
  def __init__(self, tname, nodedriver, nodeurl, nodeuser, nodepasswd, partmtd, nodeid, partcol, partparam1, partparam2):
    self.tname = tname;
    self.nodedriver = nodedriver;
    self.nodeurl = nodeurl;
    self.nodeuser = nodeuser; 
    self.nodepasswd = nodepasswd;
    self.partmtd = partmtd;
    self.nodeid = nodeid; 
    self.partcol = partcol;
    self.partparam1 = partparam1; 
    self.partparam2 = partparam2;
  def displayTable(self):
    print("\nTable Name: ", self.tname, "\nNode Driver: ", self.nodedriver, "\nNode Url: ", self.nodeurl, "\nNode User: ", self.nodeuser, "\nNode Password: ", self.nodepasswd, "\nPartition Method: ", self.partmtd, "\nNode ID: ", self.nodeid, "\nPartition Column: ", self.partcol, "\nPartition Parameter 1: ", self.partparam1, "\nPartition Parameter 2: ", self.partparam2); 

if __name__ == '__main__':
  load(argv);
