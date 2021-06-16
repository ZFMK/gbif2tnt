import logging
import logging.config

logger = logging.getLogger('sync_gbif2tnt')


import re
import pudb


from .InsertIntoTablesBase import InsertIntoTablesBase

class InsertTaxonNameListReference():
	def __init__(self, tntdbcon, temptablename):
		InsertIntoTablesBase.__init__(self, tntdbcon)
		
		self.temptable = temptablename
		
		
		self.taxonreferencetable = 'TaxonNameReferenceList'
		self.taxonnamelisttable = 'TaxonNameList'
		
		
	
	def copyTaxonReferenceTable(self):
		query = """
		
		
		
		""".format(self.taxonreferencetable, self.temptable)
		
		
		pass
		
		
		
		


