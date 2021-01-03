import random
from threading import Lock, Condition

class Monitor:
    def __init__(self):
        self.nLookups = 0
        self.nDatas = 0
        self.busy = False
        self.OKtoAddRemove = Condition() #mesle write hast
        self.OKtoLookup = Condition() #mesle read hast
        self.OKtoAddData = Condition()
        self.mutex = Lock()

    def startAddRemove(self):
        pass
    def endAddRemove(self):
        pass
    def startLookup(self):
        pass
    def endLookup(self):
        pass
    def startAddData(self):
        pass
    def endAddData(self):
        pass

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
        self.monitor = Monitor()
    
    def addToNetwork(self):
        self.monitor.startAddRemove()
        agent = Agent()
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
        self.__updateFTOnAdd(agent)
        self.monitor.endAddRemove()

    def removeFromNetwork(self, agent):
        self.monitor.startAddRemove()
        agent_index = self.nodes.index(agent)
        
        # predecessore successoresh taghir mikone
        self.nodes[(agent_index+1)%len(self.nodes)].predecessor = self.nodes[agent_index].predecessor
        # successore predecessoresh taghir mikone
        self.nodes[agent_index-1].successor = self.nodes[agent_index].successor

        # data hash montaghel mishe
        for data in self.nodes[agent_index].datas:
            self.nodes[(agent_index+1)%len(self.nodes)].datas.append(data)

        # ft ha update mishan
        self.__updateFTOnRemove(agent)
        self.monitor.endAddRemove()

    def lookUp(self, agent, key):
        self.monitor.startLookup()
        agent_index = self.nodes.index(agent)
        i = agent_index
        r = 0
        while r < 5:
            if self.nodes[i].FT[r].id == key:
                return self.nodes[i].FT[r]
            elif self.nodes[i].FT[r].id < key:
                if r != 4:
                    i = self.nodes.index(self.nodes[i].FT[r])
                    r = 0
                else:
                    r+=1
            elif self.nodes[i].FT[r].id > key:
                if r != 0:
                    i = self.nodes.index(self.nodes[i].FT[r-1])
                    r = 0
                else:
                    return self.nodes[i].FT[r]
        self.monitor.endLookup()

    def addData(self, value):
        self.monitor.startAddData()
        data = Data(value, self.nodes)
        i = 0
        while i < len(self.nodes):
            if self.nodes[i].id >= data.key:
                self.nodes[i].datas.append(data)
                break
            i+=1
        self.monitor.endAddData()
    
    def __updateFTOnAdd(self, agent):
        agent_index = self.nodes.index(agent)
        
        # FT khodesh va 5ta agent ghablesh
        agent_counter = 6
        i = agent_index
        while agent_counter > 0:
            r = 0
            while r < 5:
                key = (self.nodes[i].id + 2**r)%self.nodes[-1].id
                k = i
                flag = False
                while k < len(self.nodes):
                    if key <= self.nodes[k].id:
                        self.nodes[i].FT[r] = self.nodes[k]
                        flag = True
                        break
                    k+=1
                if not flag:
                    k = 0
                    while k < i:
                        if key <= self.nodes[k].id:
                            self.nodes[i].FT[r] = self.nodes[k]
                            flag = True
                            break
                        k+=1
                r+=1
            i-=1
            agent_counter-=1

    def __updateFTOnRemove(self, agent):
        agent_index = self.nodes.index(agent)
        # remove agent from network
        self.nodes.remove(agent)

        agent_counter = 4
        i = agent_index-1
        while agent_counter > 0:
            r = 0
            while r < 5:
                key = (self.nodes[i].id + 2**r)%self.nodes[-1].id
                k = i
                flag = False
                while k < len(self.nodes):
                    if key <= self.nodes[k].id:
                        self.nodes[i].FT[r] = self.nodes[k]
                        flag = True
                        break
                    k+=1
                if not flag:
                    k = 0
                    while k < i:
                        if key <= self.nodes[k].id:
                            self.nodes[i].FT[r] = self.nodes[k]
                            flag = True
                            break
                        k+=1
                r+=1
            i-=1
            agent_counter-=1

class Data:
    def __init__(self, value, network):
        self.key = self.__setKey(network)
        self.value = value

    def __setKey(self, network):
        new_key = random.randint(1, 5000)
        i = 0
        while i < len(network):
            r = 0
            while r < len(network[i].datas):
                if new_key == network[i].datas[r].key:
                    new_key = random.randint(1, 5000)
                    i = -1
                    break
                else:
                    r+=1
            i+=1
        return new_key
