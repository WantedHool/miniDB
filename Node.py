import sys
import time
sys.path.insert(0, '..') # Import the files where the modules are located

from MyOwnPeer2PeerNode import MyOwnPeer2PeerNode

node_1 = MyOwnPeer2PeerNode("127.0.0.1", 8001)
#node_2 = MyOwnPeer2PeerNode("127.0.0.1", 8002)
node_3 = MyOwnPeer2PeerNode("127.0.0.1", 8003)
node_4 = MyOwnPeer2PeerNode("10.0.2.15", 8004)

time.sleep(1)

node_1.start()
#node_2.start()
node_3.start()
node_4.start()

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