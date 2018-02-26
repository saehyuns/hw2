# Homework 2: Naive SQL Processing for Parallel DBMS
* [Homework 2 Page](https://lipyeow.github.io/ics421s18/morea/queryproc/experience-hw2.html)
* [Installation](#installation)
  * [Install Docker Containers](#install-docker-containers)
  * [Setup a Docker Container](#setup-a-docker-container)
  * [Setup Python](#setup-python)
  * [How to Install Homework 2 Program](#how-to-install-homework-2-program)
  * [How to Run Homework 2 Program](#how-to-run-homework-2-program)
* [Overview](#overview)
  * [Directory Structure](#directory-structure)
  * [File and Program Descriptions](#file-and-program-descriptions)
    * [Server Files](#server-files)
    * [Configuration Files](#configuration-files)
    * [Client Program](#client-program)
  * [Expected Output and Error Conditions](#expected-output-and-error-conditions)
    * [Expected Output](#expected-output)
    * [Error Conditions](#error-conditions)
* [Cheat Sheets](#cheat-sheets)
  * [Docker Cheat Sheet](#docker-cheat-sheet)
  * [Linux Cheat Sheet](#linux-cheat-sheet)

# Installation

## Install Docker Containers
First, download the Docker Community Edition (CE) for the Desktop from their [download page](https://www.docker.com/community-edition#/download).

Secondly, follow their instructions at their [Getting Started Page](https://docs.docker.com/get-started/).

If you want [hands-on tutorials](https://docs.docker.com/get-started/) or would like to use their [training videos and online playground](http://training.play-with-docker.com/), please feel free to do so.

## Setup a Docker Container
First, open up a terminal or command prompt window and cd to the designated Docker directory:
```
cd Desktop/Docker
```
After you're in your designated Docker directory, start your own container. In this case, I named my container "myContainer" and started it up with Ubuntu:
```
docker run -it --name=[myContainer] ubuntu
```
Since the base Ubuntu image only has the bare minimal packages installed, we will need to install some more packages:
```
apt-get -y update
apt-get -y install iputils-ping
apt-get -y install iproute
apt-get -y install dnsutils
```
You can find the ip address of your current container by doing ip a. For example, my container's ip is 172.17.0.2.:
```
ip a
```

```
1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN group default qlen 1
    link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00
    inet 127.0.0.1/8 scope host lo
       valid_lft forever preferred_lft forever
2: tunl0@NONE: <NOARP> mtu 1480 qdisc noop state DOWN group default qlen 1
    link/ipip 0.0.0.0 brd 0.0.0.0
3: ip6tnl0@NONE: <NOARP> mtu 1452 qdisc noop state DOWN group default qlen 1
    link/tunnel6 :: brd ::
6: eth0@if7: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue state UP group default
    link/ether 02:42:ac:11:00:02 brd ff:ff:ff:ff:ff:ff link-netnsid 0
    inet 172.17.0.2/16 brd 172.17.255.255 scope global eth0
       valid_lft forever preferred_lft forever
```
You can also ping the address of your container or other containers that you have created by using the ping:
```
ping 172.18.0.3
```
Professor Lipyeow Lim provides us with a very good demonstration of setting up a docker container and pinging:
[![OH NO SOMETHING WENT WRONG](http://img.youtube.com/vi/YHL_TaSC_hk/0.jpg)](http://www.youtube.com/watch?feature=player_embedded&v=YHL_TaSC_hk)

## Setup Python
Now that we've installed the packages we need, let's also install Python3, package installer, vim, and sqlite3:
```
apt-get -y install python3
apt-get -y install python3-pip
apt-get -y install vim
apt-get -y install sqlite3
```
Since it is not a good practice to work in root, let's create a user account:
```
adduser [username]
```
Make sure to follow the onscreen instructions!
```
su [username]
cd
```
## How to Install Homework 2 Program
Leave the terminal or command prompt open and go to my [github page](https://github.com/saehyuns/hw2). Download the zip containing my homework 2 files and store / unzip it into the designated docker directory.

Now start up a new terminal or command prompt tab or window and go to your designated docker directory, and copy the files into your container. In my case:
```
cd Desktop/Docker
chmod -R 0777 hw2-master
docker cp /Users/SaeHyunSong/Desktop/Docker/hw2-master [Container Name]:/home/[user name]/
```
Now go back to your container and cd into the hw2-master directory and voila we're all done with the installation process!

## How to Run Homework 2 Program
Go to the catalog directory and start up the server, default hostname: 127.0.0.10 and port: 5000:
```
cd hw2n0
python3 parDBd.py 127.0.0.10 5000 &
```

Go to the node1 directory and start up the server with the commandline arguments that match the cluster.cfg file's hostname and port number:
```
cd hw2n1
python3 parDBd.py 127.0.0.2 5000 &
```

Go to the node2 directory and start up the server with the commandline arguments that match the cluster.cfg file's hostname and port number:
```
cd hw2n2
python3 parDBd.py 127.0.0.3 5000 &
```

Go to the catalog directory and run the loadCSV file to insert initial contents to the database.
You can choose to run the hash partition method:
```
cd hw2n0
python3 loadCSV.py hash.cfg books.csv
```
Or the range partition method:
```
cd hw2n0
python3 loadCSV.py range.cfg books.csv
```

Now the database has been updated, now do ps to see the processes of the server running
in the background. You should see something similar to this:
```
  PID TTY          TIME CMD
 4295 pts/1    00:00:02 bash
 7500 pts/1    00:00:00 python3
 7502 pts/1    00:00:00 python3
 7505 pts/1    00:00:00 python3
 7515 pts/1    00:00:00 ps
```

Now kill all the processes that have the CMD of python3:
```
kill 7500
kill 7502
kill 7505
```

Now restart all the servers again for the catalog, node1, and node2.
You may have to wait a while for the server to completely shut down, approx 30 seconds:
```
cd hw2n0
python3 parDBd.py 127.0.0.10 5000 &

cd hw2n1
python3 parDBd.py 127.0.0.2 5000 &

cd hw2n2
python3 parDBd.py 127.0.0.3 5000 &
```

Now run the runSQL program through run.sh:
```
./run.sh cluster.cfg books.sql
```
You should then see the [expected output](#expected-output). The reason why you need
to restart the server is because the loadCSV program adds in the rows to the sqlite
table but the server does not know that. Since it's asynchronous with the database 
you would need to restart the server so that it can see the contents of the database.
This is a future implementation that I will work on.

Then run everything again from the [beginning](#how-to-run-homework-2-program).
If you get any errors, refer to the [Error Conditions Section](#error-conditions).

# Overview

## Directory Structure
The top-level directory structure contains:
```
hw2n0/          # Holds the sqlite3 database mycatdb and the server program.
  mycatdb         # The sqlite3 database called mycatdb.
  parDBd.py       # The server program for the catalog node.
  books.csv       # The csv file containing rows that will be inserted into database.
  books.sql       # The sql file containing the commands
  cluster.cfg     # Configuration for the cluster node for runSQL.py.
  hash.cfg        # Configuration for the hash partition for loadCSV.py.
  range.cfg       # Configuration for the range partition for loadCSV.py.
  run.sh          # Shell script to run the runSQL.py with commandline arguments.
  runSQL.py       # The client program that executes SQL query via multithreading
  loadCSV.py      # The client program that loads the data in csv file with a specific partition to the dbs.
  
hw2n1/            # Holds the sqlite3 database mydb1 and the server program to simulate a different computer.
  mydb1           # The sqlite3 database called mydb1.
  parDBd.py       # The server program for the node1 node.
  
hw2n2/            # Holds the sqlite3 database mydb2 and the server program to simulate a different computer.
  mydb2           # The sqlite3 database called mydb2.
  parDBd.py       # The server program for the node2 node.
  
README.md         # Contains information about installation and files.
```

## File and Program Descriptions

### Server Files
There are three server files one for each node called parDBd.py along with their own sqlite3 database:
```
hw2n0/          # Holds the sqlite3 database mycatdb and the server program.
  mycatdb         # The sqlite3 database called mycatdb.
  parDBd.py       # The server program for the catalog node.
  
hw2n1/            # Holds the sqlite3 database mydb1 and the server program.
  mydb1           # The sqlite3 database called mydb1.
  parDBd.py       # The server program for the node1 node.
  
hw2n2/            # Holds the sqlite3 database mydb2 and the server program.
  mydb2           # The sqlite3 database called mydb2.
  parDBd.py       # The server program for the node2 node.
```
The node1 and node2 directory has the same server program but different database names:
```
hw2n1/            # Holds the sqlite3 database mydb1 and the server program.
  mydb1           # The sqlite3 database called mydb1.
  parDBd.py       # The server program for the node1 node.
  
hw2n2/            # Holds the sqlite3 database mydb2 and the server program.
  mydb2           # The sqlite3 database called mydb2.
  parDBd.py       # The server program for the node2 node.
```
The parDBd.py program for hw2n1/ and hw2n1/ contains:
```
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
  mySocket.listen(100)

  while(1):
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
```
What this server program is basically doing is that it takes in two commandline arguments: IP address or hostname, and the port number. It uses those command line arguments to generate a socket with that ip address and port number. The socket will then listen for any data sent to it. Using the data it has received, it will connect to it's sqlite3 database with the data containing the database name. Execute the DDL statement that was sent along with the database name. Which then it will send a message back to the client program that it was successful or not. Then it finally closes all the connections after everything has been done. 

The catalog directory has a slightly different server program compared to the node1 and node2 servers:
```
hw2n0/          # Holds the sqlite3 database mycatdb and the server program.
  mycatdb         # The sqlite3 database called mycatdb.
  parDBd.py       # The server program for the catalog node.
```
The parDBd.py program for hw2n0/ contains: 
```
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
  mySocket.listen(100)

  while(1):
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
```
What this server program does differently compared to the other server programs for node1 and node2 is that it receives from the client that the node1 or node2 was successful. Which then it creates the table called DTABLES it id does not already exist in the catalog database. After checking that, it will store the metadata about the DDL being executed in the catalog database. Then it sends the message back to the client that the catalog has been updated. The good thing about this is that the client will only send a message to the catalog server if node1 and node2 create tables were successful, saving time. It will be explained more when I am describing the client program runDDL.py.

### Configuration Files

There are five configuration files books.csv, books.sql, cluster.cfg, hash.cfg, range.cfg:
```
cluster.cfg       # A configuration file that contains information about the nodes.
hash.cfg          # A configuration file that contains information about the hash partition.
range.cfg         # A configuration file that contains information about the range partition.
books.csv         # A comma separated value file containing information to be added to the respective dbs.
books.sql         # Contains a DDL command which is used to run on the node's database.
```

The cluster.cfg file contains access information for each computer on the cluster such as the hostname, port, database name, and the number of nodes. This file will be parsed and the data will be used to send information to the cluster of node server programs. 
```
catalog.hostname=127.0.0.1:5000/mycatdb

numnodes=2

node1.hostname=127.0.0.2:5000/mydb1

node2.hostname=127.0.0.3:5000/mydb2
```

The hash.cfg file contains information for the hash partition. This file will be parsed
and the data/parameters will be used to determine which db the values in the csv file will go to.
```
catalog.driver=com.ibm.db2.jcc.DB2Driver
catalog.hostname=127.0.0.10:5000/mycatdb
catalog.username=db2inst1
catalog.passwd=mypasswd

tablename=books

partition.method=hash
partition.column=isbn
partition.param1=2
```

The range.cfg file contains information for the range partition. This file will be parsed
and the data/parameters will be used to determine which db the values in the csv file will go to.
```
catalog.driver=com.ibm.db2.jcc.DB2Driver
catalog.hostname=127.0.0.10:5000/mycatdb
catalog.username=db2inst1
catalog.passwd=mypasswd

tablename=books
partition.method=range
partition.column=isbn

numnodes=2
partition.node1.param1=100000000
partition.node1.param2=200000000

partition.node2.param1=200000000
partition.node2.param2=300000000
```

The file books.sql contains the DDL terminated by a semi-colon to be executed on the node server:
```
SELECT * FROM BOOKS;
```


The file books.csv contains the values to be added into the database.
```
123323232,"Database Systems","Ramakrishnan, Raghu"
234323423,"Operating Systems","Silberstein, Adam"
```

### Client Program

The client program called runSQL.py contains:
```
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
    mySocket.close();

    print(receivedp[3]);

    print("[" + url + "]: " + receivedp[0]);
    if(receivedp[0] == "./books.sql success."):
      message = receivedp[0] + "$" + tablename + "$" + url;
      mySocket2 = socket.socket();
      mySocket2.connect((catalogNode.hostname, int(catalogNode.port)));
      mySocket2.send(message.encode());
      received = mySocket2.recv(1024).decode();
      print("[" + catalogNode.url + "]: " + received);
      mySocket2.close();

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
```
The runDDL program takes in two commandline arguments which are cluster.cfg and books.sql. It will then parse the books.sql file and insert it into an array called ddlCommands. It also will parse the cluster.cfg file and store them into an array called data. Which then I create a loop for each data to split them and place each value in their respective variables and append that data into a Node class. Then I store each instance of the Node class that was created into the nodes array. Finally, I go through each each node and send the data that was parsed from the cluster.cfg and books.sql file to the node servers CONCURRENTLY USING THREADS and print out the message I receive from the server of whether or not it was successful. If it was successful, it will send that data to the catalog node to update the database there with the metadata. Otherwise, it will not send anything and just print out that the catalog was not updated. 

## Expected Output and Error Conditions

### Expected Output
The expected output for loadCSV.py should be:
```
[127.0.0.2:5000/mydb1]: ./books.sql success.
[127.0.0.2:5000/mydb1]: 1 rows inserted.
[127.0.0.10:5000/mycatdb]: catalog updated.
[127.0.0.3:5000/mydb2]: ./books.sql success.
[127.0.0.3:5000/mydb2]: 1 rows inserted.
[127.0.0.10:5000/mycatdb]: catalog updated.
```

The expected output for runSQL.py should be:
```
[(123323232, Database Systems, Ramakrishnan, Raghu)]
[127.0.0.2:5000/mydb1]: ./books.sql success.
[(234323423, Operating Systems, Silberstein, Adam)]
[127.0.0.3:5000/mydb2]: ./books.sql success.
[127.0.0.10:5000/mycatdb]: catalog updated.
[127.0.0.10:5000/mycatdb]: catalog updated.
DONE
```

### Error Conditions
An error condition would be if the ip address or hostname is inputted wrong / doesn't match the cluster.cfg file when using commandline arguments, it will give a connection refused error:
```
Traceback (most recent call last):
  File "runDDL.py", line 100, in <module>
    runDDL(argv);
  File "runDDL.py", line 64, in runDDL
    mySocket.connect((nodes[x].hostname, int(nodes[x].port)));
ConnectionRefusedError: [Errno 111] Connection refused
```

Another error condition would be if the server is not finished shutting down, it will say that the server ip address is already in use. I'd suggest waiting about a minute to restart the server:
```
  File "parDBd.py", line 53, in <module>
    Main(argv);
  File "parDBd.py", line 21, in Main
    mySocket.bind((str(host),int(port)))
OSError: [Errno 98] Address already in use
```

# Cheat Sheets

## Docker Cheat Sheet
Here is a simple [Docker Cheat Sheet](https://www.docker.com/sites/default/files/Docker_CheatSheet_08.09.2016_0.pdf) provided by Docker in case you're unfamiliar with the commands.

## Linux Cheat Sheet
Here is a [Linux/Unix Commands Reference](https://files.fosswire.com/2007/08/fwunixref.pdf) in case you're unfamiliar with the system as we will be using a Docker Linux Container with Ubuntu.
