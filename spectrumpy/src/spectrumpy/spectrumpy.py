#
# Copyright 2019 Pitney Bowes Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0 
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing 
# permissions and limitations under the License.
#
#
#from __future__ import (absolute_import, print_function)
import os
from pathlib import Path
import sys
import re
import requests
import time
from datetime import timedelta
from datetime import datetime
import zeep
import lxml
import urllib
from urllib.parse import quote
from urllib.error import HTTPError
import base64
from xml.dom.minidom import parse, parseString, Document
import configparser
import json

def Info(msg):
	time = datetime.now().strftime('%H:%M:%S')
	print ('{0} INFO: {1}'.format(time, msg))

def Warning(msg):
	time = datetime.now().strftime('%H:%M:%S')
	print ('{0} WARNING: {1}'.format(time, msg))

def Error(msg):
	time = datetime.now().strftime('%H:%M:%S')
	print ('{0} ERROR: {1}'.format(time, msg))


def GetHttpContent(url):
	request = urllib.request.Request(url)
	return urllib.request.urlopen(request).read()

class HttpConnection:
	def __init__(self):
		self.CallLogPath = 'CallLog.txt'
		self.Username = ''
		self.Password = ''
		self.Url = ''
		self.RequestType = ''
		# Initializing the data to an empty string '' makes this a POST request
		self.Data = None
		self.ContentType = ''
		self.Log = True
		self.ReturnError = False

	def Send(self):

		#
		# Configure connection header.
		#
		
		opener = urllib.request.build_opener(urllib.request.HTTPHandler)
		request = urllib.request.Request(self.Url, data = self.Data)
		base64string = base64.encodestring('{0}:{1}'.format(self.Username, self.Password).encode()).decode().replace('\n', '')
		request.add_header('Authorization', 'Basic %s' % base64string)
		request.add_header('Content-Type', self.ContentType)
		request.add_header('Accept', self.ContentType)
		request.get_method = lambda: self.RequestType.upper()
		
		#
		# Attempt to send.
		#

		result = None
		try:

			#
			# Execute HTTP request and record time.
			#

			startTime = datetime.now()
			result = opener.open(request)
			runtime = (datetime.now() - startTime)
			result = result.read()
			

			#
			# Write execute time and other information to log file.
			#

			if self.Log:
				with open(self.CallLogPath, 'a') as file:
					file.write('URL: {0}\nUSER: {8}\nPW: {9}\nTYPE: {6}\nBYTES SENT: {1}\nBYTES RECEIVED: {2}\nDATASENT: {4}\nRESPONSE: {5}\nRUNTIME: {3}\nTIME: {7}\n\n'
					.format(self.Url, len(str(self.Data)) * 4, len(result) * 4, runtime, str(self.Data), result, self.RequestType, datetime.now(), self.Username, self.Password))

		#
		# Capture error.
		#

		except urllib.error.URLError as e:
			if self.ReturnError:
				return str(e) + ' - ERROR!'
			with open(self.CallLogPath, 'a') as file:
				file.write('URL: {0}\nUSER: {4}\nPW: {5}\nTYPE: {2}\nDATASENT: {1}\nTIME: {3}\n\n'
				.format(self.Url, str(self.Data), self.RequestType, datetime.now(), self.Username, self.Password))
			Error('REST API ERROR: {0}\n{1}'.format(e, self.Url))
		return result

class APIManager:

	def __init__(self, apiUrl, doDebug):
		self.debug = doDebug

		#
		# Declare public objects.
		#

		self.Url = apiUrl
		self.Resources = {}
		self.Objects = {}

		#
		# Process WADL XML content.
		#
		wadl = GetHttpContent(self.Url)
		wadlDom = parseString(wadl)

		if (self.debug == True):
			print ('WADL:')
			print (wadl)
			# print (wadlDom.toprettyxml(indent="  "))
			print ('')
			print ('Parsing WADL for defined methods...')

		self.__ProcessAPIXml(wadlDom, None, None, None)
		self.__CombineObjectAttributes()

	#
	# Define recursive helper function for combining parent/child attributes. 
	#

	def __CombineObjectAttributesHelp(self, base2):
		attributes = list(self.Objects[base2]['attributes'])
		for base3 in self.Objects[base2]['bases']:
			attributes += list(self.__CombineObjectAttributesHelp(base3))
		return attributes

	#
	# Define initial function for combining parent/child attributes. 
	#

	def __CombineObjectAttributes(self):
		for objectA in self.Objects:
			for base1 in self.Objects[objectA]['bases']:
				self.Objects[objectA]['attributes'] += list(self.__CombineObjectAttributesHelp(base1))

	#
	# Define function for processing WADL grammar pages. 
	#

	def __ProcessGrammerXml(self, node, complexType):
		if (node == None):
			return

		#
		# Capture initial complexType node.
		#

		if (node.nodeName == 'xs:complexType'):
			complexType = node.getAttributeNode('name').nodeValue
			self.Objects[complexType] = {}
			self.Objects[complexType]['attributes'] = []
			self.Objects[complexType]['bases'] = []
			self.Objects[complexType]['elements'] = []
		elif not complexType is None:

			#
			# Capture attribute node.
			#

			if (node.nodeName == 'xs:attribute'):
				self.Objects[complexType]['attributes'].append(node.getAttributeNode('name').nodeValue)

			#
			# Capture extension object (inherited object).
			#

			elif (node.nodeName == 'xs:extension'):
				baseName = node.getAttributeNode('base').nodeValue
				baseName = re.sub('tns\:', '', baseName)
				self.Objects[complexType]['bases'].append(baseName)

			#
			# Capture element object.
			#

			elif (node.nodeName == 'xs:element'):
				nameNode = node.getAttributeNode('name')
				if not (nameNode == None):
					self.Objects[complexType]['elements'].append(nameNode.nodeValue)

		#
		# Continue to process child nodes recursively.
		#

		for childNode in node.childNodes:
			self.__ProcessGrammerXml(childNode, complexType)

	def __ProcessAPIXml(self, node, parent, currentUrl, currentApiName):
		if (node == None):
			return

		#
		# Capture include node, which will contain object definitions.
		#

		if (node.nodeName == 'include'): 
			grammerhref = re.sub('[^/]+\Z', '', self.Url) + node.getAttributeNode('href').nodeValue
			self.__ProcessGrammerXml(parseString(GetHttpContent(grammerhref)), None)

		#
		# Capture resources node, which will contain the base URL.
		#

		elif (node.nodeName == 'resources'):
			self.base = currentUrl = re.sub('/\Z', '', node.getAttributeNode('base').nodeValue)

		#
		# Capture resource node, which will contain a base API object.
		#

		elif (node.nodeName == 'resource'):
			currentUrl += node.getAttributeNode('path').nodeValue

		#
		# Capture method node, which will be a API call for the current parent.
		#

		elif (node.nodeName == 'method'):
			requestType = node.getAttributeNode('name').nodeValue
			method = re.sub('(\A/)|(/\Z)', '', currentUrl.replace(self.base, ''))
			method = '{0}_{1}'.format(re.sub('/', '_', method), requestType)
			currentApiName = method.replace('.','_')
			if (self.debug == True):
				print ('   Processing method: {0}, apiName: {1}, method: {2}, requestType: {3}'.format(node.getAttributeNode('name').nodeValue, currentApiName, method, requestType))
			self.Resources[currentApiName] = {}
			self.Resources[currentApiName]['requesttype'] = requestType
			self.Resources[currentApiName]['contentType'] = 'application/xml'
			self.Resources[currentApiName]['method'] = method
			self.Resources[currentApiName]['url'] = currentUrl
			self.Resources[currentApiName]['params'] = {}

		#
		# Capture XML response and request node, will be the request (object) type and response type of current APL call.
		#

		elif ((node.nodeName == 'ns2:representation') or (node.nodeName == 'representation')):
			if not (node.getAttributeNode('mediaType') is None):
				self.Resources[currentApiName]['contentType'] = node.getAttributeNode('mediaType').nodeValue
			if not (node.getAttributeNode('element') is None):
				if (node.getAttributeNode('mediaType').nodeValue == 'application/xml'):
					if (parent.nodeName == 'request'):
						self.Resources[currentApiName]['xmlrequest'] = node.getAttributeNode('element').nodeValue
					else:
						self.Resources[currentApiName]['xmlresponse'] = node.getAttributeNode('element').nodeValue
				
		#
		# Capture URL parameter node, will be a parameter for the URL.
		#

		elif (node.nodeName == 'param'):
			if not (currentApiName is None):
				name = node.getAttributeNode('name').nodeValue
				typeA = node.getAttributeNode('type').nodeValue
				arg = name.replace(".", "_");
				if (self.debug == True):
					print ('      Parameter {0}, name: {1}, type: {2}'.format(arg, name, typeA))
				self.Resources[currentApiName]['params'][arg] = {'name':name, 'type':typeA}

		#
		# Continue to process child nodes recursively.
		#
		
		for childNode in node.childNodes:
			self.__ProcessAPIXml(childNode, node, currentUrl, currentApiName)

	def DisplayHelp(self):
		innerSelf = self
		for resource in innerSelf.Resources:
			if (resource == 'results_json_GET'):
				resource = innerSelf.Resources[resource]
				print ('RequestType: {0}'.format(resource['requesttype']))
				print ('ContentType: {0}'.format(resource['contentType']))
				print ('Method: {0}'.format(resource['method']))
				print ('URL: {0}'.format(resource['url']))
				print ('Arguments:')
				paramDict = resource['params']
				for param in paramDict:
					print ('   {0} : {1} => {2}'.format(param, paramDict[param]['type'], paramDict[param]['name']))
	
		
	def GetConnection(self, username, password, returnError = False):
		innerSelf = self

		#
		# Define decorator fuction which is used to force the "resource" parameter into API calls.
		#

		def createFuction(resource):
			def wrap(function):
				def wrapped(* args, ** kwargs):
					return function(args, kwargs, resource)
				return wrapped
			return wrap

		#
		# Define connection class using META programming.
		#

		class Connection:
			def __init__ (self, doDebug):
				self.debug = doDebug
				self.Apis = {}
				self.username = username
				self.password = password
				self.ReturnError = returnError
				for resource in innerSelf.Resources:
					#method = innerSelf.Resources[resource]['method']
					
					#
					# Define API fuction.
					#

					@createFuction(resource)
					def api(args, kwargs, resource):

						#
						# Create connection object.
						#

						connection = HttpConnection()
						connection.Username = self.username
						connection.Password = self.password
						connection.ContentType = innerSelf.Resources[resource]['contentType']
						connection.RequestType = innerSelf.Resources[resource]['requesttype']
						connection.ReturnError = self.ReturnError
						connection.Url = innerSelf.Resources[resource]['url']

						#
						# Check and set connection.
						#

						firstUrlArg = True
						requestXmlObject = None if not 'xmlrequest' in innerSelf.Resources[resource] else innerSelf.Resources[resource]['xmlrequest']
						xmlElements = ''
						xmlAttributes = ''
						for argument in kwargs:
						
							#
							# Check if argument is URL parameter.
							#

							if argument in innerSelf.Resources[resource]['params']:
								firstChar = '?' if firstUrlArg else '&'
								connection.Url += '{0}{1}={2}'.format(firstChar, innerSelf.Resources[resource]['params'][argument]['name'], quote(kwargs[argument],safe=''))
								firstUrlArg = False
								continue

							#
							# Check if argument is XML parameter.
							#
	
							if not (requestXmlObject is None):
								if argument in innerSelf.Objects[requestXmlObject]['attributes']:
									xmlAttributes += ' {0}="{1}"'.format(argument, kwargs[argument])
									continue
								if argument in innerSelf.Objects[requestXmlObject]['elements']:
									xmlElements += '<{0}>{1}</{0}>'.format(argument, kwargs[argument])
									continue

							#
							# At this point throw error because given parameter was not found.
							#

							Error('Invalid API argument API: {0}, Argument {1}={2}'.format(resource, argument,kwargs[argument]))

						#
						# Append XML meta data if any XML parameters were added.
						#

						if not requestXmlObject is None:
							connection.Data = '<?xml version="1.0" ?><{0}{1}>{2}</{0}>'.format(requestXmlObject, xmlAttributes, xmlElements)

						#
						# Send request.
						#
						if (self.debug == True):
							print ('Request URL: {0}'.format(connection.Url))
							if not requestXmlObject is None:
								print ('Request payload: {0}'.format(connection.Data))

						return connection.Send()

					#
					# Set function call and information.
					#

					self.__dict__[resource]=api
					self.Apis[resource] = api
		return Connection(self.debug)


class Servers:
	def _read_config_(config):
		home = str(Path.home())
		config.read( (os.path.dirname(__file__) + '\servers.ini', home + '\.spectrum_servers.ini', os.getcwd() + '\.spectrum_servers.ini'))
	
	def getAvailableServers():
		"""Returns array of names of all known Spectrum servers. """
		config = configparser.ConfigParser()
		Servers._read_config_(config)
		servers=[]
		for key in config['SERVERS']: servers.append(config['SERVERS'][key])
		return servers
		
	def getServer(name, debug = False):
		"""A the Server object for the specified name. """
		config = configparser.ConfigParser()
		Servers._read_config_(config)
		if config.has_section(name):
			doDebug = debug
			server=Server(config[name]['url'], (config[name]['user'],config[name]['pwd']), debug=doDebug)
			return server
		return None
				
class Server:
	def __init__(self):
		self.url='http://localhost:8080/'
		self.credentials=('admin','admin')
		self.Services = {}
		self.spectrumServices = None
		self.debug = False
		
	def __init__(self, url, credentials, debug = False):
		''' Constructor for this class. '''
		self.url=url
		self.credentials=credentials
		self.Services = {}
		self.spectrumServices = None
		doDebug = debug
		self.debug=doDebug
		#self.__AddRestServices()
		
	def __GetRestServices(self):
		opener = urllib.request.build_opener(urllib.request.HTTPHandler)
		request = urllib.request.Request(self.url + 'rest')
		base64string = base64.encodestring('{0}:{1}'.format(self.credentials[0], self.credentials[1]).encode()).decode().replace('\n', '')
		request.add_header('Authorization', 'Basic %s' % base64string)
		result = None
		result = opener.open(request)
		encoding = result.headers.get_content_charset('utf-8')
		result = result.read().decode(encoding)
		#if (self.debug == True):
		#	print ('Spectrum Rest services: {0}'.format(result))

		# HACK!!! The html returned from Spectrum is non-standard and the python DOM parser doesn't like it, these should fix it up
		result = result.replace('<HEAD>','<head>')
		result = result.replace('<HTML>','<html>')
		result = result.replace('"><meta','"/><meta')
		result = result.replace('"><title','"/><title')
		if (self.debug == True):
			print ('Spectrum Rest services: {0}'.format(result))

		return result
	
	def __ProcessRestServices(self, node, parent, currentUrl):
		if (node == None):
			if (self.debug == True):
				print('Node is empty, returning')
			return
			
		if ((node.nodeType == node.TEXT_NODE) and (parent.nodeName == 'a') and (node.nodeValue.endswith('?_wadl'))):
			service_name = node.nodeValue.replace(self.url + 'rest/','').replace('?_wadl','')
			if (self.debug == True):
				print ('Service {0} : {1}'.format(service_name, node.nodeValue))
			self.Services[service_name] = node.nodeValue
		
		for childNode in node.childNodes:
			self.__ProcessRestServices(childNode, node, currentUrl)
	
	def SpectrumServices(self):
		if self.spectrumServices == None:
			innerSelf = self
			dom = parseString(self.__GetRestServices())
			if (self.debug == True):
				print (dom.toprettyxml(indent="  "))

			self.__ProcessRestServices(dom,None,None)
			
			#
			# Define decorator fuction which is used to force the services into functions
			#
			def createFuction(service):
				def wrap(function):
					def wrapped(* args, ** kwargs):
						if (self.debug == True):
							print ('Requested Service: {0}, supplied arguments:'.format(service))
							for arg in args:
								print("   arg:", arg)
							for key, value in kwargs.items():
								print("   {0} = {1}".format(key, value))
							print ('')
						return function(args, kwargs, service)
					return wrapped
				return wrap

			def createHelpFuction(service):
				def wrap(function):
					def wrapped():
						return function(service)
					return wrapped
				return wrap

			#
			# Define SpectrumServices class using META programming.
			#
			class SpectrumServices:
				
				def __init__ (self, doDebug):
					self.debug = doDebug
					self.Apis = {}
					self.ApiHelps = {}
					
					for service in innerSelf.Services:
						try:
							@createFuction(service)
							def api(args, kwargs, service):
								api_url = innerSelf.Services[service]
								# APIManager for this URL (rest service) is where the wadl document is actually fetched and parsed
								apiManager = APIManager(api_url, self.debug)
								# GetConnection is where the supplied args are formatted into a request URL and opened
								self.connection = apiManager.GetConnection(innerSelf.credentials[0], innerSelf.credentials[1])	
								response = self.connection.results_json_GET(**kwargs)
								if not response is None:
									response = response.decode('utf-8')
									if (self.debug == True):
										print (response)
								return response
								
							@createHelpFuction(service)
							def apiHelp(service):
								api_url = innerSelf.Services[service]
								# APIManager for this URL (rest service) is where the wadl document is actually fetched and parsed
								apiManager = APIManager(api_url, self.debug)
								apiManager.DisplayHelp()	
								
							self.__dict__[service]=api
							self.__dict__['Help_'+service]=apiHelp
							self.Apis[service] = api
							self.ApiHelps[service] = apiHelp
						except:
							# TODO: Some WADLs are more complex - e.g. Spatial is the wadl for Spectrum Spatial rest services
							pass

				def Help(self, service):
					self.__dict__['Help_'+service]()

			self.spectrumServices = SpectrumServices(self.debug)
			
		return self.spectrumServices
		
	def url(self):
		return self.url
		
	def get(self, path):
		try:
			response = requests.get(self.url+path, auth=self.credentials)
			return response
		except requests.exceptions.RequestException as e:
			print (e)

	def getSoapService(self, wsdl):
		session = requests.Session()
		session.auth = requests.auth.HTTPBasicAuth(self.credentials[0], self.credentials[1])
		soapService = zeep.Client(self.url+wsdl, transport=zeep.Transport(session=session))
		return soapService
	

	

