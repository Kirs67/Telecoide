import json
import socket
import threading
import time

Modules = {}
ModulesLock = threading.Lock()

buses = {}
busesLock = threading.Lock()

lastMesID = 0
lastMesIDLock = threading.Lock()


class Connection:

	def disconnectionRoutine(module):
		module.connection = None


	def connectionRoutine(connect):
		client, address = connect

		uuid = client.recv(36).decode('utf-8')
		module = Core.getModuleByUUID(uuid)

		module.connection = client
		moduleServiceThread = threading.Thread(target = module.service, name = "Module " + module.uuid)
		moduleServiceThread.start()

	def awaitConnection(port):
		routerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		routerSocket.bind(('127.0.0.1', port))
		routerSocket.listen()

		while True:
			Connection.connectionRoutine( routerSocket.accept() )




class TelecoideModule:
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
						print(jsonString)
						message = json.loads(jsonString)
						print(message)
						message["mesid"] = Core.newMesID

						Core.route(message) #FIXME переписать потокобезопасно и в отдельном потоке

					time.sleep(0.01)

			except OSError:
				Connection.disconnectionRoutine(self)


	def sendMessage(self, message):
		with self.lock:
			if self.isSubscribed(message):
				messageString = json.dumps(message)
				self.connection.send( messageString.encode('utf-8') )


	def isSubscribed(self, message):
		correctBusSubs = {}
		correctBusSubs.update( self.subsriptions.get( None ) )
		correctBusSubs.update( self.subsriptions.get( message["bus"] ) )

		correctTypeSubs = {}
		correctTypeSubs.update( correctBusSubs.get(None) )
		correctTypeSubs.update( correctBusSubs.get( message["type"] ) )

		return correctTypeSubs.get( None ) or correctTypeSubs.get( message["subtype"] )



class Core(TelecoideModule):

	# Функции, чтобы вести себя, как модуль (для обработки сообщений о подписке)
	def __init__(self):
		super(Core, self).__init__("00000000-0000-0000-0000-000000000000")

		with ModulesLock:
			Modules["00000000-0000-0000-0000-000000000000"] = self

		Core.registerModuleSubscription( self, { "bus":"core", "type":None, "subtype":None } )

	def service(self):
		return

	def sendMessage(self, message):
		print("Core have a message!")
		if message["bus"] == "core" and message["type"] == "subscription":
			
			if message["subtype"] == "add":
				Core.registerModuleSubscription( Modules[ message[ "uuid" ] ], message[ "payload" ])

			#elif message["subtype"] == "remove": TODO реализовать отписку


	# Сервисная функциональность
	def registerModuleSubscription(module, message):
		
		busName = message.get("bus")
		typeName = message.get("type")
		subtypeName = message.get("subtype")

		with busesLock:

			if buses.get( busName ) == None:
				buses[ busName ] = []

			buses[ busName ].append(module)

			print(buses)


		with module.lock:

			busDict = module.subsriptions.get( busName )
			if busDict == None:
				module.subsriptions[busName] = {}
				busDict = module.subsriptions[busName]

			typeDict = busDict.get( typeName )
			if typeDict == None:
				busDict[typeName] = {}
				typeDict = busDict[typeName]

			typeDict[ subtypeName ] = True		
		
			for module in Modules.values():
				print(module.subsriptions)



	def route(message):
		with busesLock: #TODO немного костыльненько, имеет смысл попробовать переписать
			subscribersBus = buses.get( message["bus"] )
			subscribersAny = buses.get( None )
	
		subscribers = []
		if subscribersBus != None:
			subscribers += subscribersBus

		if subscribersAny != None:
			subscribers += subscribersAny

		if subscribers == None:
			return

		for module in subscribers:
			module.sendMessage(message)


	def newMesID():
		id = 0;
		with lastMesIDLock:
			lastMesID += 1
			id = lastMesID

		return id

	def getModuleByUUID(uuid):
		with ModulesLock:
			module = Modules.get(uuid)
			if module == None:
				module = TelecoideModule(uuid)
				Modules[uuid] = module

		return module

#On start
Core()
Connection.awaitConnection(54231)