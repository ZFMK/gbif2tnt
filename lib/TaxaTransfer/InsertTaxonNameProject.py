import logging
import logging.config

logger = logging.getLogger('sync_gbif2tnt')

import pudb


from .InsertIntoTablesBase import InsertIntoTablesBase

class InsertTaxonNameProjectBase(InsertIntoTablesBase):
	def __init__(self, tntdbcon, temptablename, projectid):
		InsertIntoTablesBase.__init__(self, tntdbcon)
		
		self.projectid = projectid
		
		self.temptable = temptablename
		
		
		self.taxonprojecttable = 'TaxonNameProject'
		self.taxonnametable = 'TaxonName'
		
		#pudb.set_trace()
		self.insertTaxonNameProject()
	
	def insertTaxonNameProject(self):
		query = """
		INSERT INTO [{0}] (
		[NameID],
		[ProjectID]
		)
		SELECT 
		[NameID]
		,{1} AS [ProjectID]
		FROM [{2}]
		""".format(self.taxonprojecttable, self.projectid, self.temptable)
		
		self.cur.execute(query)
		self.con.commit()
		
		
		pass
		
		
		
		


