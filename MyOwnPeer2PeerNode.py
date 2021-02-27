from p2pnetwork.node import Node
from database import Database

class MyOwnPeer2PeerNode(Node):
    nodes = [["127.0.0.1", 8002], ["127.0.0.1", 8003]]
    # Python class constructor
    def __init__(self,tables):
        host = "127.0.0.1"
        port = 8001
        self.tables = tables
        super(MyOwnPeer2PeerNode, self).__init__(host, port, None)
        print("MyPeer2PeerNode: Started")

    # all the methods below are called when things happen in the network.
    # implement your network node behavior to create the required functionality.

    def outbound_node_connected(self, node):
        print("outbound_node_connected: " + node.id)

    def inbound_node_connected(self, node):
        print("inbound_node_connected: " + node.id)

    def inbound_node_disconnected(self, node):
        print("inbound_node_disconnected: " + node.id)

    def outbound_node_disconnected(self, node):
        print("outbound_node_disconnected: " + node.id)

    def node_message(self, node, data):
        message = data
        if(message["Data"]):
            self.DataHandler(message)
        else:
            if(message["action"] == "select"):
                self.select_get(message)
            elif(message["action"] == "update"):
                self.update_get(message)
            elif(message["action"] == "delete"):
                self.delete_get(message,node)
            elif(message["action"] == "insert"):
                self.insert_get(message)
            else:
                print("Invalid Message")
        print("node_message from " + node.id + ": " + str(data))

    def node_disconnect_with_outbound_node(self, node):
        print("node wants to disconnect with other outbound node: " + node.id)

    def node_request_to_stop(self):
        print("node is requested to stop!")

    def DataHandler(self):
        print("")
    def select_post(self):
        message = {
            "action" : "select",
            "table" : "table",
            "columns": []
                }
        print("")
    def select_get(self,message):
        print("")
    def insert_post(self):
        print("")
    def insert_get(self,message):
        print("")
    def delete_post(self, table_name, condition):
        message = {
            "action":"delete",
            "table":table_name,
            "condition":condition
        }
        for n in self.nodes:
            nod = MyOwnPeer2PeerNode(n[0],n[1])
            self.connect_with_node(nod)
            self.send_to_node(nod,message)
            self.disconnect_with_node(nod)
    def delete_get(self,message,node):
        db = Database('smdb',load=True)
        if message["table_name"] in db.tables:
            db.delete(message["table_name"],message["condition"])
            response = {
                "Data": self.host + " " + self.port + " :" + " Done"
            }
            self.send_to_node(node, response)
        else:
            response = {
                "Data": self.host + " " + self.port + " :" + " No work needs to be done from here"
            }
            self.send_to_node(node, response)
    def update_post(self,table_name, set_value, set_column, condition):
        message = {
            "action": "update",
            "table": table_name,
            "set_value":set_value,
            "set_column":set_column,
            "condition": condition
        }
        for n in self.nodes:
            nod = MyOwnPeer2PeerNode(n[0],n[1])
            self.connect_with_node(nod)
            self.send_to_node(nod,message)
            self.disconnect_with_node(nod)
    def update_get(self,message):
        db = Database('smdb', load=True)
        db.update(message["table_name"],message["set_value"],message["set_column"],message["condition"])
