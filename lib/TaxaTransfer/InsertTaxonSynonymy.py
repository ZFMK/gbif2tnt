import logging
import logging.config

logger = logging.getLogger('sync_gbif2tnt')

import pudb


from .InsertIntoTablesBase import InsertIntoTablesBase

class InsertTaxonSynonymyBase(InsertIntoTablesBase):
	def __init__(self, tntdbcon, temptablename, projectid):
		InsertIntoTablesBase.__init__(self, tntdbcon)
		
		self.temptable = temptablename
		
		self.projectid = projectid
		
		self.taxonsynonymytable = 'TaxonSynonymy'
		
		#pudb.set_trace()
		self.insertTaxonSynonymy()
	
	def insertTaxonSynonymy(self):
		logger.info("InsertTaxonSynonymy: inserting taxon synonymy")
		
		query = """
		INSERT INTO [{0}] (
		[ProjectID]
		,[NameID]
		,[SynNameID]
		,[IgnoreButKeepForReference]
		)
		SELECT
		{1} AS [ProjectID],
		tt1.[NameID], -- the NameIDs have been copied from TaxonName table into temporary table by InsertTaxonName class, thus they can be used here
		tt2.[NameID],
		0 AS [IgnoreButKeepForReference]
		FROM [{2}] tt1
		INNER JOIN [{2}] tt2 ON (tt1.[GBIFAcceptedTaxonID] = tt2.[GBIFTaxonID])
		WHERE tt1.[GBIFTaxonomicStatus] IN ('synonym', 'proparte synonym', 'heterotypic synonym', 'homotypic synonym')
		;""".format(self.taxonsynonymytable, self.projectid, self.temptable)
		
		self.cur.execute(query)
		self.con.commit()
		
		pass
		
		
		
		


