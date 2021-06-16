import logging
import logging.config

logger = logging.getLogger('sync_gbif2tnt')


import re
import pudb


class InsertIntoTablesBase():
	'''
	Parent class for methods used by all inserts into tnt tables
	'''
	
	def __init__(self, tntdbcon):
		
		self.tntdbcon = tntdbcon
		
		self.con = self.tntdbcon.getConnection()
		self.cur = self.tntdbcon.getCursor()
		
	
	
	
	def setValuesFromLists(self, dataslice):
		self.values = []
		for valuelist in dataslice:
			self.values.extend(valuelist)
		
		
		
	def setPlaceholderStrings(self, dataslice):
		"""
		extra method to get a list(!) of placeholder strings to be able to combine them with fixed values like NULL in the VALUES lists of the INSERT query
		"""
		self.placeholderstrings = []
		for valuelist in dataslice:
			placeholders = ['?'] * len(valuelist)
			self.placeholderstrings.append(', '.join(placeholders))
	
	def setPlaceholderString(self, dataslice):
		self.setPlaceholderStrings(dataslice)
		self.placeholderstring = '(' + '), ('.join(self.placeholderstrings) + ')'
	
	
	
	
	def getMaxID(self, targettable, idcolumn):
		query = """SELECT ISNULL(MAX([{0}]), 0) FROM [{1}]
		;""".format(idcolumn, targettable)
		self.cur.execute(query)
		row = self.cur.fetchone()
		maxid = row[0]
		return maxid
	



