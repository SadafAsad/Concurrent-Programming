class Process:
    def __init__(self, network):
        self.id = self.__setID(network)
        self.successor = self.setSuccessor(network)
        self.predecessor = self.setPredecessor(network)
        self.FT = self.setFT(network)

    def __setID(self, network):
        return 0

    def setSuccessor(self, network):
        return 0

    def setPredecessor(self, network):
        return 0

    def setFT(self, network):
        return 0

class Network:
    def __init__(self):
        self.nodes = list()
    
    def addToNetwork(self, id):
        pass

    def removeFromNetwork(self, id):
        pass

    def lookUp(self, key):
        pass

class Data:
    def __init__(self, data_keys, value):
        self.key = self.__setKey(data_keys)
        self.value = value

    def __setKey(self, data_keys):
        return 0
