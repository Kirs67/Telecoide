import json
import socket
import threading
import time

Modules = {}
ModulesLock = threading.Lock()

class TelecoideModule(object):
	uuid = ''
	connection = None

	def __init__(self, uuid):
		self.uuid = uuid

	def service(self):
		if self.connection != None:
			try:
				while True:
					byte = bytearray(1000)
					count = self.connection.recv_into(byte)
					if count != 0:
						jsonString = byte.decode('utf-8').replace('\x00', '')
						self.connection.send(str( Modules ).encode('utf-8'))

					time.sleep(0.01)

			except OSError:
				Connection.disconnectionRoutine(self)

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

#On start
Connection.awaitConnection(54231)