import random
from threading import Lock, Condition, Thread

class Monitor:
    def __init__(self):
        self.nLookups = 0
        self.nDatas = 0
        self.busy = False
        self.OKtoAddRemove = Condition() #mesle write hast
        self.OKtoLookup = Condition() #mesle read hast
        self.OKtoAddData = Condition()
        self.mutex = Lock()

    # vaghti add ya remove darim nabayad kare dg ei anjam beshe
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

    # faghat lookup ha mitoonan hamzamn anjam beshan, baghiye kara nemitoonan anjam beshan
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
    
    # vaghti ke data ei mikhad add beshe, hich kare dg ei nemitoone anjam beshe
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
        new_key = random.randint(1, 30)
        i = 0
        while i < len(network):
            r = 0
            while r < len(network[i].datas):
                if new_key == network[i].datas[r].key:
                    new_key = random.randint(1, 30)
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
        self.datas = []
        self.ft = []

# har network az majmooei node tashkil shode ke hamoon agent hamoon hastan
# va yek monitor dare baraye hamravandi
class Network:
    def __init__(self):
        self.nodes = list()
        self.monitor = Monitor()
    
    # baraye add kardan agent jadid be shabake hast
    def addToNetwork(self):
        # moghei ke darim agent add mikonim, hich kare dg ei (add,remove,lookup,addData) nemitoonim anjam bedim
        self.monitor.startAddRemove()

        # agent jadid ro misazim
        agent = Agent()

        # id random behesh midam
        new_id = random.randint(1, 30)
        i = 0
        while i < len(self.nodes):
            if self.nodes[i].id == new_id:
                new_id = random.randint(1, 30)
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
        # moghei ke darim remove mikonim hich kare dg ei (add,remove,lookup,addData) nemitoone etefagh biufte
        self.monitor.startAddRemove()

        # dombale agent i hastim ke id ish tooye variable voroodi dade behemoon
        agent_index = 0
        while agent_index < len(self.nodes):
            if self.nodes[agent_index].id == agent:
                break
            else:
                agent_index+=1
        
        # predecessore successoresh taghir mikone
        self.nodes[(agent_index+1)%len(self.nodes)].predecessor = self.nodes[agent_index].predecessor
        # successore predecessoresh taghir mikone
        self.nodes[agent_index-1].successor = self.nodes[agent_index].successor

        # data hash montaghel mishe
        for data in self.nodes[agent_index].datas:
            self.nodes[(agent_index+1)%len(self.nodes)].datas.append(data)

        # ft ha update mishan va agent az shabake remove mishe
        self.__updateFTOnRemove(self.nodes[agent_index])
        
        self.monitor.endAddRemove()

    # id oon node i ke data ro dare return mikone
    def lookUp(self, agent, key):
        # vaghti ke lookup dare anjam mishe, faghat lookup ha mitoonan ba ham kar anjam bedan, baghiye kara (add,remove,addData) nemitoonan anjam beshan
        self.monitor.startLookup()

        # dombale agent i hastim ke id ish tooye variable voroodi dade behemoon
        agent_index = 0
        while agent_index < len(self.nodes):
            if self.nodes[agent_index].id == agent:
                break
            else:
                agent_index+=1
        
        i = agent_index
        # andaze ft 5tast, ke ba variable r in ro control mikonim
        r = 0
        while r < 5:
            # agar node i vojood dashte bashe ke id ish ba key data yeki bashe, pas mishe hamoon node
            if self.nodes[i].ft[r].id == key:
                print("Data key: "+str(key)+" in Agent: "+str(self.nodes[i].ft[r].id))
                return self.nodes[i].ft[r]
            # agar ke id node koochik tar az key data bashe 2 halat vojood dare:
            elif self.nodes[i].ft[r].id < key:
                # agar ke in node akharin node tooye ft boode bashe, pas yani bayad berim tooye ft in begardim
                if r == 4:
                    i = self.nodes.index(self.nodes[i].ft[r])
                    r = 0
                # agar ke hanooz node dg ei tooye ft hast, pas mire baghiye ft ro check mikone
                else:
                    r+=1
            # agar ke id node bozorg tar az key data bashe 2 halat vojood dare:
            elif self.nodes[i].ft[r].id > key:
                # agar ke avalin node tooye ft naboode, mirim node ghablish ke yani id ish koochi tar boode bar midarim va tooye ft oon migardim
                # in hamoon halati hast ke migim doroste ft ro darim vali nemitoonim ghatei begim beyne 2ta node, node dg ei boode ya na, pas hatman tooye ft oon koochike mirim migardim
                if r != 0:
                    i = self.nodes.index(self.nodes[i].ft[r-1])
                    r = 0
                # inja dg e in avalin node tooye ft boode va id ish bozorg tar az key data hast, pas mishe hamin node
                else:
                    print("Data key: "+str(key)+" in Agent: "+str(self.nodes[i].ft[r].id))
                    return self.nodes[i].ft[r]
       
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
    
    # moghei ke agent jadidi be shabake add mishe, ft khodesh bayad sakhte beshe va ft yek seri agent dge ham bayad taghir kone
    def __updateFTOnAdd(self, agent):
        # index agent jadid ke be shabake add shode aro migiram
        agent_index = self.nodes.index(agent)

        # FT khodesh va 5ta agent ghablesh
        agent_counter = 6

        # andaze ft ro 5 gereftam va ba variable r control mikonm
        i = agent_index
        # abs(i) baraye moghei hast ke rooye dayere shabake dare harekat mikone va ba index manfi hast, check mikonm ke index esh nazane biroon
        while agent_counter > 0 and abs(i)<len(self.nodes)+1:
            self.nodes[i].ft = []
            r = 0
            while r < 5:
                # in key hamoon chizi hast ke badan check mikonim bebinim che node i mitoone in key ro dashte bashe ...
                key = (self.nodes[i].id + 2**r)%self.nodes[-1].id
                # az avale shabake check mikone bebine avalin node i ke key imoon koochi kar mosavi id ish hast kojast
                k = 0
                while k < len(self.nodes):
                    if key <= self.nodes[k].id:
                        if k < len(self.nodes):
                            self.nodes[i].ft.append(self.nodes[k])
                        break
                    k+=1
                r+=1
            i-=1
            agent_counter-=1

    # vaghti ke agent i remove mishe bayad ft yek seri agent update beshe
    def __updateFTOnRemove(self, agent):
        # index agent i ke mikhad remove beshe ro az shabake migirim
        agent_index = self.nodes.index(agent)

        # remove agent from network
        self.nodes.remove(agent)

        # ft 5ta agent ghablesh bayad update beshe
        agent_counter = 4

        # index avalin agent i ke ghablesh boode va bayad ft ish update beshe
        i = agent_index-1
        # abs(i) baraye moghei hast ke rooye dayere shabake dare harekat mikone va ba index manfi hast, check mikonm ke index esh nazane biroon
        while agent_counter > 0 and abs(i)<len(self.nodes)+1:
            self.nodes[i].ft = []
            r = 0
            while r < 5:
                # in key hamoon chizi hast ke badan check mikonim bebinim che node i mitoone in key ro dashte bashe ...
                key = (self.nodes[i].id + 2**r)%self.nodes[-1].id
                # az avale shabake check mikone bebine avalin node i ke key imoon koochi kar mosavi id ish hast kojast
                k = 0
                while k < len(self.nodes):
                    if key <= self.nodes[k].id:
                        if k < len(self.nodes):
                            self.nodes[i].ft.append(self.nodes[k])
                        break
                    k+=1
                r+=1
            i-=1
            agent_counter-=1

    def printChord(self):
        print("sadaf")
        for agent in self.nodes:
            print("Agent: "+str(agent.id)+" Successor: "+str(agent.successor.id)+" Predecessor: "+str(agent.predecessor.id))
            print("Datas: ", end="")
            for data in agent.datas:
                print("[k:"+str(data.key)+" v:"+str(data.value)+"] ", end="")
            print("\n"+"FT: ", end="")
            for node in agent.ft:
                print(str(node.id)+" ", end="")
            print("\n--------------------------------------------------------------------------------------------------------------")

if __name__ == '__main__':
    network = Network()

    print("How many agents do you want in your chord?")
    agent_count = int(input())
    add_agent_list = list()

    print("How many datas do you want to add to your chord?")
    print("***datas will be generated randomly***")
    data_count = int(input())
    add_data_list = list()

    printLock = Lock()

    for r in range(agent_count):
        add_agent_list.append(Thread(target=network.addToNetwork))
    for r in range(data_count):
        add_data_list.append(Thread(target=network.addData, args=[random.randint(0,100)]))
    
    for i in range(len(add_agent_list)):
        add_agent_list[i].start()
    for i in range(len(add_data_list)):
        add_data_list[i].start()

    for i in range(len(add_agent_list)):
        add_agent_list[i].join()
    for i in range(len(add_data_list)):
        add_data_list[i].join()

    network.printChord()

    search_list = list()
    print("Do you want to search a data? 1)yes 2)no")
    print("***your agent id must be less than your data key***")
    c = int(input())
    while c == 1:
        print("Enter agent id: ", end="")
        agent_id = int(input())
        print("Enter data key: ", end="")
        data_key = int(input())
        search_list.append(Thread(target=network.lookUp, args=[agent_id, data_key]))
        print("Do you want to continue searching? 1)yes 2)no")
        c = int(input())

    for i in range(len(search_list)):
        search_list[i].start()
    for i in range(len(search_list)):
        search_list[i].join()

    remove_list = list()
    print("Do you want to remove an agent? 1)yes 2)no")
    c= int(input())
    while c == 1:
        print("Enter agent id: ", end="")
        agent_id = int(input())
        remove_list.append(Thread(target=network.removeFromNetwork, args=[agent_id]))
        print("Do you want to continue removing? 1)yes 2)no")
        c = int(input())

    for i in range(len(remove_list)):
        remove_list[i].start()
    for i in range(len(remove_list)):
        remove_list[i].join()
    
    network.printChord()


