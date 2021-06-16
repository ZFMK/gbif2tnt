import logging
import logging.config

logger = logging.getLogger('query')


from .InsertIntoTablesBase import InsertIntoTablesBase

import re
import pudb


class InsertCommonNameTempTableBase(InsertIntoTablesBase):
	def __init__(self, tntdbcon, namesgetter):
		InsertIntoTablesBase.__init__(self, tntdbcon)
		
		self.namesgetter = namesgetter
		
		self.temptable = '#TempCommonName'
		
		self.commonnames = []
		self.createCommonNameTempTable()
		self.fillCommonNameTempTable()
		
		
	def createCommonNameTempTable(self):
		query = """SELECT TOP 0
			IDENTITY (INT) as rownumber, -- set an IDENTITY column that can be used for paging
			cn.[CommonName],
			cn.[LanguageCode],
			cn.[CountryCode],
			cn.[ReferenceTitle]
			INTO [{0}]
			FROM [TaxonCommonName] cn
			;""".format(self.temptable)
		
		logger.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
			ALTER TABLE [{0}]
			ADD [GBIFTaxonID] INT
			;""".format(self.temptable)
		logger.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
			CREATE INDEX [GBIFTaxonID_idx] ON [{0}] ([GBIFTaxonID])
			;""".format(self.temptable)
		logger.info(query)
		self.cur.execute(query)
		self.con.commit()
	
	
	def fillCommonNameTempTable(self):
		self.commonnames = self.namesgetter.getNextTaxaPage()
		
		while self.commonnames is not None:
			
			self.setPlaceholderString(self.commonnames)
			self.setValuesFromLists(self.commonnames)
			
			query = """
			INSERT INTO [{0}]
			(
			[GBIFTaxonID],
			[CommonName],
			[LanguageCode],
			[CountryCode],
			[ReferenceTitle]
			)
			VALUES {1}
			;""".format(self.temptable, self.placeholderstring)
			
			
			logger.info(query)
			#logger.info(self.values)
			
			
			self.cur.execute(query, self.values)
			self.con.commit()
			
			self.commonnames = self.namesgetter.getNextTaxaPage()
		
	
	
	
	
	def getTempTableName(self):
		return self.temptable
		
		



