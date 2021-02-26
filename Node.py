import sys
import time
sys.path.insert(0, '..') # Import the files where the modules are located

from MyOwnPeer2PeerNode import MyOwnPeer2PeerNode

class Node:

    address ="127.0.0.1"
    port = 8001
    node = MyOwnPeer2PeerNode(address, port)
    nodes = [["127.0.0.1",8002],["127.0.0.1",8003]]
    def __init__(self):
        self.node.start()
    def select_post(self):
        for n in self.nodes:
            self.node.connect_with_node(n[0], n[1])
            self.node.send_to_node(MyOwnPeer2PeerNode(n[0],n[1]),"select")
            self.node.disconnect_with_node(MyOwnPeer2PeerNode(n[0],n[1]))
    def insert(self):
        for n in self.nodes:
            self.node.send_to_node(MyOwnPeer2PeerNode(n[0],n[1]),"insert")
    def delete(self):
        for n in self.nodes:
            self.node.send_to_node(MyOwnPeer2PeerNode(n[0],n[1]),"delete")
    def update(self):
        for n in self.nodes:
            self.node.send_to_node(MyOwnPeer2PeerNode(n[0],n[1]),"update")
    def Handler(self):
        while(True):
            self.node.node_message()
    time.sleep(1)

    node.start()

    time.sleep(1)

    node_1.connect_with_node('127.0.0.1', 8002)
    node_1.connect_with_node('127.0.0.1', 8003)
    node_1.connect_with_node('127.0.0.1', 8004)
    node_4.connect_with_node('85.72.101.98',8002)

    time.sleep(2)

    node_4.send_to_nodes("Geia so kokla")

    time.sleep(5)

    node_1.stop()
    #node_2.stop()
    node_3.stop()
    node_4.stop()
    print('end test')