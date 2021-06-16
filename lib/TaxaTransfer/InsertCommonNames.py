import logging
import logging.config

logger = logging.getLogger('sync_gbif2tnt')


import re
import pudb


# from ..MSSQLConnector import MSSQLConnector
from .InsertCommonNameTempTable import InsertCommonNameTempTableBase



class InsertCommonNames():
	def __init__(self, tnt_con, taxonnametemptable, vernaculargetter):
		self.vernaculargetter = vernaculargetter
		
		self.tntdbcon = tnt_con
		self.cur = self.tntdbcon.getCursor()
		self.con = self.tntdbcon.getConnection()
		
		self.taxonnametemptable = taxonnametemptable
		
		
		self.insertCommonNames()
	
	
	
	def insertCommonNames(self):
		temptableimporter = InsertCommonNameTempTableBase(self.tntdbcon, self.vernaculargetter)
		self.commonnametemptable = temptableimporter.getTempTableName()
		
		
		logger.info("InsertCommonNames")
		
		query = """
		INSERT INTO [TaxonCommonName] (
		[NameID],
		[CommonName],
		[LanguageCode],
		[CountryCode],
		[ReferenceTitle]
		)
		SELECT DISTINCT
		ttn.[NameID],
		tcn.[CommonName],
		tcn.[LanguageCode],
		tcn.[CountryCode],
		tcn.[ReferenceTitle]
		FROM [{0}] tcn 
		INNER JOIN [{1}] ttn
		ON (ttn.GBIFTaxonID = tcn.GBIFTaxonID)
		INNER JOIN [LanguageCode_Enum] le
		ON (le.[Code] = tcn.[LanguageCode])
		INNER JOIN [CountryCode_Enum] ce
		ON (ce.[Code] = tcn.[CountryCode])
		""".format(self.commonnametemptable, self.taxonnametemptable)
		
		#logger.info(query)
		
		self.cur.execute(query)
		self.con.commit()
		
		
	




