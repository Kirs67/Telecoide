import json
import socket
import threading
import time

def debug(arg):
	print(arg)

class Connection:

	def disconnectionRoutine(module):
		with module.lock:
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

	def __init__(self, uuid):
		self.uuid = uuid
		self.connection = None
		self.subscriptions = {} # вида { "bus1":{ "type11":{ "subtype111":True } } , "bus2":{ "type21":{ "subtype211":True } , "type22":{ "subtype221":True , None:True } } }

		self.lock = threading.Lock()

	def service(self):
		if self.connection != None:
			try:
				while True:
					byte = bytearray(1000) #TODO немного костыльненько, имеет смысл попробовать переписать
					count = self.connection.recv_into(byte)
					if count != 0:
						jsonString = byte.decode('utf-8').replace('\x00', '')						
						message = json.loads(jsonString)
						message["mesid"] = Core.newMesID()

						debug("Got message")
						debug(message)
						debug("")

						Core.route(message) #FIXME переписать потокобезопасно и в отдельном потоке

					time.sleep(0.01)

			except OSError:
				Connection.disconnectionRoutine(self)


	def sendMessage(self, message):
		
		debug("Sending message")
		debug(message)
		debug("To module")
		debug(self.uuid)
		debug("")

		messageString = json.dumps(message)
		self.connection.send( messageString.encode('utf-8') )



class Core(TelecoideModule):

	modules = {}
	modulesLock = threading.Lock()

	buses = {}
	busesLock = threading.Lock()

	lastMesID = 0
	lastMesIDLock = threading.Lock()

	# Функции, чтобы вести себя, как модуль (для обработки сообщений о подписке)

	def __init__(self):
		super(Core, self).__init__("00000000-0000-0000-0000-000000000000")

		with Core.modulesLock:
			Core.modules["00000000-0000-0000-0000-000000000000"] = self

		Core.registerModuleSubscription( self, { "bus":"core", "type":"any", "subtype":"any" } )

	def service(self):
		return

	def sendMessage(self, message):

		debug("Core have a message")
		debug(message)
		debug("")

		if message["bus"] == "core" and message["type"] == "subscription":
			
			if message["subtype"] == "add":
				Core.registerModuleSubscription( Core.getModuleByUUID( message[ "uuid" ] ), message[ "payload" ])

			#elif message["subtype"] == "remove": TODO реализовать отписку


	# Сервисная функциональность

	def registerModuleSubscription(module, message):
		
		busName = message["bus"]
		typeName = message["type"]
		subtypeName = message["subtype"]

		debug("Registering subscription")
		debug(busName)
		debug(typeName)
		debug(subtypeName)
		debug(module.uuid)
		debug("")

		with Core.busesLock:

			if Core.buses.get( busName ) == None:
				Core.buses[ busName ] = []

			if module not in Core.buses[ busName ]:
				Core.buses[ busName ].append(module)

			debug("Buses dict")
			debug(Core.buses)
			debug("")


		with module.lock:

			moduleSubs = module.subscriptions

			busDict = moduleSubs.get( busName )
			if busDict == None:
				moduleSubs[busName] = {}
				busDict = moduleSubs[busName]

			typeDict = busDict.get( typeName )
			if typeDict == None:
				busDict[typeName] = {}
				typeDict = busDict[typeName]

			typeDict[ subtypeName ] = True		
		
			debug("Subs dict of module with uuid")
			debug(module.uuid)
			debug(module.subscriptions)
			debug("")



	def route(message):
		with Core.busesLock: #TODO немного костыльненько, имеет смысл попробовать переписать
			subscribersBus = Core.buses.get( message["bus"] )
			subscribersAny = Core.buses.get( "any" )
	
		subscribers = []
		if subscribersBus != None:
			subscribers += subscribersBus

		if subscribersAny != None:
			subscribers += subscribersAny

		if subscribers == None:
			return

		for module in subscribers:
			with module.lock:
				if Core.isModuleSubscribed(module, message):
					module.sendMessage(message)

	def isModuleSubscribed(module, message):
		subs = module.subscriptions

		correctBusSubs = {}

		try:
			correctBusSubs.update( subs.get( "any" ) )
		except TypeError:
			pass

		try:
			correctBusSubs.update( subs.get( message["bus"] ) )
		except TypeError:
			pass

		correctTypeSubs = {}

		try:
			correctTypeSubs.update( correctBusSubs.get( "any" ) )
		except TypeError:
			pass

		try:
			correctTypeSubs.update( correctBusSubs.get( message["type"] ) )
		except TypeError:
			pass

		if ( correctTypeSubs.get( "any" ) != None ) or ( correctTypeSubs.get( message["subtype"] ) != None ):
			return True

		return False

	def newMesID():
		id = 0;
		with Core.lastMesIDLock:
			Core.lastMesID += 1
			id = Core.lastMesID

		return id

	def getModuleByUUID(uuid):
		with Core.modulesLock:
			module = Core.modules.get(uuid)
			if module == None:
				module = TelecoideModule(uuid)
				Core.modules[uuid] = module

		return module

#On start
Core() # Инстанцируем ядерный модуль
Connection.awaitConnection(54231)