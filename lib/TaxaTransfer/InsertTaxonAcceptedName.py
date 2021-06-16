import logging
import logging.config

logger = logging.getLogger('sync_gbif2tnt')

import pudb


from .InsertIntoTablesBase import InsertIntoTablesBase

class InsertTaxonAcceptedNameBase(InsertIntoTablesBase):
	def __init__(self, tntdbcon, temptablename, projectid):
		InsertIntoTablesBase.__init__(self, tntdbcon)
		
		self.temptable = temptablename
		
		self.projectid = projectid
		
		self.acceptednametable = 'TaxonAcceptedName'
		
		#pudb.set_trace()
		self.insertTaxonAcceptedName()
	
	def insertTaxonAcceptedName(self):
		logger.info("InsertTaxonAcceptedName: inserting accepted name table")
		
		query = """
		INSERT INTO [{0}] (
		[ProjectID]
		,[NameID]
		,[IgnoreButKeepForReference]
		)
		SELECT
		{1} AS [ProjectID],
		tt1.[NameID],
		0 AS [IgnoreButKeepForReference]
		FROM [{2}] tt1
		WHERE tt1.[GBIFTaxonomicStatus] = 'accepted'
		;""".format(self.acceptednametable, self.projectid, self.temptable)
		
		self.cur.execute(query)
		self.con.commit()
		
		
		


