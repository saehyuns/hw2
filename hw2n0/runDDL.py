# importing socket library to do the socket connections between server and client.
import socket

# Importing sqlite3 library to do sqlite3 functions.
import sqlite3
from sqlite3 import Error

# Importing sys library to taking in commandline arguments.
import sys
from sys import argv

def runDDL(argv):
    # Read from the cluster.cfg file and store it into an array called data.
    data = [];
    configFile = open(argv[1], "r");
    data = configFile.read().strip().replace("\n",";").split(';');
    configFile.close();

    # Read from the books.sql file and store the DDL into ddlCommands array.
    data = list(filter(('').__ne__, data));
    ddlCommands = [];
    ddlFile = open(argv[2], "r");
    ddlCommands= ddlFile.read().strip().replace("\n","").split(';');
    ddlFile.close();

    # Initialize variables to store data in.
    url = '';
    hostname = '';
    port = '';
    db = '';
    numnodes = 0;
    nodes = [];

    # Parse the data in the cluster.cfg file and store them into their respective variables
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

    # Get the number of nodes and find the tablename / message.
    numnodes = len(nodes);
    tablename = ddlCommands[0].split("(")[0].split(" ")[2];
    message = nodes[1].url.split("/", 1)[1];

    # Loop through each node and send a message to each node's server.
    x = 1;
    while(x < numnodes):
        message = nodes[x].url.split("/", 1)[1];
        message = "/" + message + "$" + ddlCommands[0];
        mySocket = socket.socket();
        mySocket.connect((nodes[x].hostname, int(nodes[x].port)));
        mySocket.send(message.encode())
        received = mySocket.recv(1024).decode();
        receivedp = received.split("$");
        mySocket.close();
        # Receive a message back from the server, if success send a command to the catalog server to create a table / store data.
        if receivedp[0] == "./books.sql success.":
            print("[" + nodes[x].url + "]: " + receivedp[0]);
            message2 = receivedp[0] + "$" + tablename + "$" + nodes[x].url;
            mySocket2 = socket.socket();
            mySocket2.connect((nodes[0].hostname, int(nodes[0].port)));
            mySocket2.send(message2.encode());
            received2 = mySocket2.recv(1024).decode();
            # If the catalog has been updated print that it has been. 
            if received2 == "catalog updated.": 
                print("[" + nodes[0].url + "]: " + received2);
            else:
                print("[" + nodes[0].url + "]: " + received2);
            mySocket2.close();
        # If not a success, print that the catalog has not been updated.
        else:  
            print("[" + nodes[x].url + "]: " + receivedp[0]);
            print("[" + nodes[0].url + "]: catalog not updated.");
        x += 1;

# A class called node containing the url, hostname, port, and db name of the node.
class Node:
    def __init__(self, url, hostname, port, db):
        self.url = url;
        self.hostname = hostname;
        self.port = port;
        self.db = db;
    def displayNode(self):
        print("URL:", self.url, "HOSTNAME:", self.hostname, "PORT:", self.port, "DB:", self.db);

# Run the function runDDL with 2 commandline arguments.
runDDL(argv);
