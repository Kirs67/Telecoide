import json
import socket
import threading
import time

Modules = {}
ModulesLock = threading.Lock()

Buses = {}
BusesLock = threading.Lock()

lastMesID = 0
lastMesIDLock = threading.Lock()

class TelecoideModule(object):
	uuid = ''
	connection = None
	subsriptions = {} # вида { "bus1":{ "type11":{ "subtype111":True } } , "bus2":{ "type21":{ "subtype211":True } , "type22":{ "subtype221":True , None:True } } }

	lock = threading.Lock()

	def __init__(self, uuid):
		self.uuid = uuid

	def service(self):
		if self.connection != None:
			try:
				while True:
					byte = bytearray(1000) #TODO немного костыльненько, имеет смысл попробовать переписать
					count = self.connection.recv_into(byte)
					if count != 0:
						jsonString = byte.decode('utf-8').replace('\x00', '')
						message = json.loads(jsonString)
						message["mesid"] = Core.newMesID

					time.sleep(0.01)

			except OSError:
				Connection.disconnectionRoutine(self)



	def isSubscribed(self, message):
		correctBusSubs = {}
		correctBusSubs.update( self.subsriptions.get( None ) )
		correctBusSubs.update( self.subsriptions.get( message["bus"] ) )

		correctTypeSubs = {}
		correctTypeSubs.update( correctBusSubs.get(None) )
		correctTypeSubs.update( correctBusSubs.get( message["type"] ) )

		return correctTypeSubs.get( None ) or correctTypeSubs.get( message["subtype"] )
			

class Connection:

	def disconnectionRoutine(module):
		module.connection = None


	def connectionRoutine(connect):
		client, address = connect

		uuid = client.recv(36).decode('utf-8')
		module = Connection.getModuleByUUID(uuid)

		module.connection = client
		moduleServiceThread = threading.Thread(target = module.service, name = "Module " + module.uuid)
		moduleServiceThread.start()

	def getModuleByUUID(uuid):
		with ModulesLock:
			module = Modules.get(uuid)
			if module == None:
				module = TelecoideModule(uuid)
				Modules[uuid] = module

		return module


	def awaitConnection(port):
		routerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		routerSocket.bind(('127.0.0.1', port))
		routerSocket.listen()

		while True:
			Connection.connectionRoutine( routerSocket.accept() )

class Core:

	def route(message):
		with BusesLock: #TODO немного костыльненько, имеет смысл попробовать переписать
			subscribersBus = Buses.get( message["bus"] )
			subscribersAny = Buses.get( None )
	
		subscribers = []
		if subscribersBus != None:
			subscribers += subscribersBus

		if subscribersAny != None:
			subscribers += subscribersAny

		if subscribers == None:
			return

		for module in subscribers:
			with module.lock:
				if module.isSubscribed(message):
					messageString = json.dumps(message)
					module.connection.send( messageString.encode('utf-8') )


	def newMesID():
		id = 0;
		with lastMesIDLock:
			lastMesID++
			id = lastMesID

		return id

#On start
Connection.awaitConnection(54231)