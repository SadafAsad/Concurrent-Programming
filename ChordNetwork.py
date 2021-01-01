import random

class Agent:
    def __init__(self):
        self.id = None
        self.successor = None
        self.predecessor = None
        self.datas = list()
        self.FT = list()

class Network:
    def __init__(self):
        self.nodes = list()
    
    def addToNetwork(self, agent):
        # inja lock mishe
        # id random behesh midam
        new_id = random.randint(1, 5000)
        i = 0
        while i < len(self.nodes):
            if self.nodes[i].id == new_id:
                new_id = random.randint(1, 5000)
                i = 0
            else:
                i+=1
        agent.id = new_id
        # be shabake ezaf mikonm
        self.nodes.append(agent)
        self.nodes.sort(key=lambda agent: agent.id)

        agent_index = self.nodes.index(agent)
        # successoresh ro moshakhas mikonm
        self.nodes[agent_index].successor = self.nodes[(agent_index+1)%len(self.nodes)]
        # predecessore successoresh ro taghir midam
        self.nodes[(agent_index+1)%len(self.nodes)].predecessor = self.nodes[agent_index]
        # predecessoresh ro moshakhas mikonm
        self.nodes[agent_index].predecessor = self.nodes[agent_index-1]
        # successore predecessoresh ro taghir midam
        self.nodes[agent_index-1].successor = self.nodes[agent_index]

        # data behesh midam
        for data in self.nodes[agent_index].successor.datas:
            if data.key <= self.nodes[agent_index].id:
                self.nodes[agent_index].datas.append(data)
                self.nodes[agent_index].successor.datas.remove(data)

        # ft ha update mishan
        # lock ta inja

    def removeFromNetwork(self, agent):
        # inja lock mishe
        agent_index = self.nodes.index(agent)
        
        # predecessore successoresh taghir mikone
        self.nodes[(agent_index+1)%len(self.nodes)].predecessor = self.nodes[agent_index].predecessor
        # successore predecessoresh taghir mikone
        self.nodes[agent_index-1].successor = self.nodes[agent_index].successor

        # data hash montaghel mishe
        for data in self.nodes[agent_index].datas:
            self.nodes[(agent_index+1)%len(self.nodes)].datas.append(data)

        # ft ha update mishan
        # az shabake hazf mishe
        self.nodes.remove(agent)
        # lock ta inja

    def lookUp(self, key):
        pass

    def addData(self, data):
        # inja lock mishe
        i = 0
        while i < len(self.nodes):
            if self.nodes[i].id >= data.key:
                self.nodes[i].datas.append(data)
                break
            i+=1
        # lock ta inja
    
    def updateFTOnAdd(self, agent):
        pass

    def updateFTOnRemove(self, agent):
        pass

class Data:
    def __init__(self, value, data_keys):
        self.key = self.__setKey(data_keys)
        self.value = value

    def __setKey(self, data_keys):
        # inja lock mishe
        new_key = random.randint(1, 5000)
        i = 0
        while i < len(data_keys):
            if new_key == data_keys[i]:
                new_key = random.randint(1, 5000)
                i = 0
            else:
                i+=1
        data_keys.append(new_key)
        # lock ta inja
        return new_key
