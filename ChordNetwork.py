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
        self.OKtoAddRemove.acquire()
        self.mutex.acquire()
        while self.busy or self.nDatas > 0 or self.nLookups > 0:
            self.mutex.release()
            self.OKtoAddRemove.wait()
            self.mutex.acquire()
        self.busy = True
        self.mutex.release()
        self.OKtoAddRemove.release()
    def endAddRemove(self):
        self.OKtoAddRemove.acquire()
        self.OKtoAddData.acquire()
        self.OKtoLookup.acquire()
        self.mutex.acquire()
        self.busy = False
        self.mutex.release()
        self.OKtoAddRemove.notify()
        self.OKtoAddRemove.release()
        self.OKtoAddData.notify()
        self.OKtoAddData.release()
        self.OKtoLookup.notify()
        self.OKtoLookup.release()

    def startLookup(self):
        self.OKtoLookup.acquire()
        self.mutex.acquire()
        while self.busy:
            self.mutex.release()
            self.OKtoLookup.wait()
            self.mutex.acquire()
        self.nLookups += 1
        self.mutex.release()
        self.OKtoLookup.notifyAll()
        self.OKtoLookup.release()
    def endLookup(self):
        self.OKtoLookup.acquire()
        self.mutex.acquire()
        self.nLookups -= self.nLookups
        if self.nLookups == 0:
            self.OKtoAddData.acquire()
            self.OKtoAddData.notify()
            self.OKtoAddData.release()

            self.OKtoAddRemove.acquire()
            self.OKtoAddRemove.notify()
            self.OKtoAddRemove.release()
        self.mutex.release()
        self.OKtoLookup.release()
    
    def startAddData(self):
        self.OKtoAddData.acquire()
        self.mutex.acquire()
        while self.busy:
            self.mutex.release()
            self.OKtoAddData.wait()
            self.mutex.acquire()
        self.nDatas += 1
        self.mutex.release()
        self.OKtoAddData.notifyAll()
        self.OKtoAddData.release()
    def endAddData(self):
        self.OKtoAddData.acquire()
        self.mutex.acquire()
        self.nDatas -= self.nDatas
        if self.nDatas == 0:
            self.OKtoLookup.acquire()
            self.OKtoLookup.notify()
            self.OKtoLookup.release()

            self.OKtoAddRemove.acquire()
            self.OKtoAddRemove.notify()
            self.OKtoAddRemove.release()
        self.mutex.release()
        self.OKtoAddData.release()

# data hamoon ye value daran va ye key
# Data(5,network) --> data ei misaze ba value 5 va key ham khodesh khodkar adadi tooye range (1,5000) be soorate random mide behesh ke unique hast
# network ro migire, ke bebine in data dare baraye che networki sakhte mishe ke key data haye tooye oon network unique beshan
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

# har agent 5ta field dare: id, successor, predecessor, datas, ft
class Agent:
    def __init__(self):
        self.id = None
        self.successor = None
        self.predecessor = None
        self.datas = list()
        self.FT = list()

# har network az majmooei node tashkil shode ke hamoon agent hamoon hastan
# va yek monitor dare baraye hamravandi
class Network:
    def __init__(self):
        self.nodes = list()
        self.monitor = Monitor()
    
    # baraye add kardan agent jadid be shabake hast
    def addToNetwork(self):
        # moghei ke darim agent add mikonim, hich kare dg ei (add,remove,lookup,...) nemitoonim anjam bedim
        self.monitor.startAddRemove()

        # agent jadid ro misazim
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

        # agent haye network ro sort mikonm
        self.nodes.sort(key=lambda agent: agent.id)

        # index agent am ke sakhte shode va be network sort shode add shode ro migiram
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

    # baraye remove kardan agent az shabake hast
    def removeFromNetwork(self, agent):
        # moghei ke darim remove mikonim hich kare dg ei (add,remove,lookup,...) nemitoone etefagh biufte
        self.monitor.startAddRemove()

        # index agent i ke mikhaym remove konim ro az shabake peyda mikonim
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

    # id oon node i ke data ro dare return mikone
    def lookUp(self, agent, key):
        self.monitor.startLookup()
        agent_index = self.nodes.index(agent)
        i = agent_index
        r = 0
        while r < 5:
            # agar node i vojood dashte bashe ke id ish ba key data yeki bashe, pas mishe hamoon node
            if self.nodes[i].FT[r].id == key:
                return self.nodes[i].FT[r]
            # agar ke id node koochik tar az key data bashe 2 halat vojood dare:
            elif self.nodes[i].FT[r].id < key:
                # agar ke in node akharin node tooye ft boode bashe, pas yani bayad berim tooye ft in begardim
                if r == 4:
                    i = self.nodes.index(self.nodes[i].FT[r])
                    r = 0
                # agar ke hanooz node dg ei tooye ft hast, pas mire baghiye ft ro check mikone
                else:
                    r+=1
            # agar ke id node bozorg tar az key data bashe 2 halat vojood dare:
            elif self.nodes[i].FT[r].id > key:
                # agar ke avalin node tooye ft naboode, mirim node ghablish ke yani id ish koochi tar boode bar midarim va tooye ft oon migardim
                # in hamoon halati hast ke migim doroste ft ro darim vali nemitoonim ghatei begim beyne 2ta node, node dg ei boode ya na, pas hatman tooye ft oon koochike mirim migardim
                if r != 0:
                    i = self.nodes.index(self.nodes[i].FT[r-1])
                    r = 0
                # inja dg e in avalin node tooye ft boode va id ish bozorg tar az key data hast, pas mishe hamin node
                else:
                    return self.nodes[i].FT[r]
        self.monitor.endLookup()

    # baraye add kardan data be shabake hast
    def addData(self, value):
        # vaghti darim data be shabake add mikonim hich kare dg ei nemitoone etefagh biufte
        self.monitor.startAddData()

        # data jadid sakhte mishe
        data = Data(value, self.nodes)

        # data ro be shabake ezaf mikonim
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
