import logging
import logging.config

logger = logging.getLogger('sync_gbif2tnt')

import pudb


from .InsertIntoTablesBase import InsertIntoTablesBase

class InsertTaxonHierarchyBase(InsertIntoTablesBase):
	def __init__(self, tntdbcon, temptablename, projectid):
		InsertIntoTablesBase.__init__(self, tntdbcon)
		
		self.temptable = temptablename
		
		self.projectid = projectid
		
		self.taxonhierarchytable = 'TaxonHierarchy'
		
		#pudb.set_trace()
		self.insertTaxonHierarchy()
	
	def insertTaxonHierarchy(self):
		logger.info("InsertTaxonHierarchy: inserting taxon hierarchy")
		
		query = """
		INSERT INTO [{0}] (
		[ProjectID]
		,[NameID]
		,[NameParentID]
		,[IgnoreButKeepForReference]
		)
		SELECT
		{1} AS [ProjectID],
		tt1.[NameID], -- the NameIDs have been copied from TaxonName table into temporary table by InsertTaxonName class, thus they can be used here
		tt2.[NameID],
		0 AS [IgnoreButKeepForReference]
		FROM [{2}] tt1
		INNER JOIN [{2}] tt2 ON (tt1.[GBIFParentTaxonID] = tt2.[GBIFTaxonID])
		WHERE tt1.[GBIFTaxonomicStatus] = 'accepted'
		;""".format(self.taxonhierarchytable, self.projectid, self.temptable)
		
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
		 -- insert Hierarchy for synonyms too, should they be calculated from JOIN with accepted taxon or
		 -- shouldn't it just take over the parent taxa from GBIF?
		INSERT INTO [{0}] (
		[ProjectID]
		,[NameID]
		,[NameParentID]
		,[IgnoreButKeepForReference]
		)
		SELECT
		{1} AS [ProjectID],
		tt1.[NameID], -- the NameIDs have been copied from TaxonName table into temporary table by InsertTaxonName class, thus they can be used here
		tt3.[NameID],
		0 AS [IgnoreButKeepForReference]
		FROM [{2}] tt1
		INNER JOIN [{2}] tt2 ON (tt1.[GBIFAcceptedTaxonID] = tt2.[GBIFTaxonID])
		INNER JOIN [{2}] tt3 ON (tt2.[GBIFParentTaxonID] = tt3.[GBIFTaxonID])
		WHERE tt1.[GBIFTaxonomicStatus] IN ('synonym', 'proparte synonym', 'heterotypic synonym', 'homotypic synonym')
		;""".format(self.taxonhierarchytable, self.projectid, self.temptable)
		
		self.cur.execute(query)
		self.con.commit()
		
		
		
		#pudb.set_trace()
		# add NULL as NameParentID for taxa that have no parent (Animalia, Plantae, Fungi)
		query = """
		INSERT INTO [{0}] (
		[ProjectID]
		,[NameID]
		,[NameParentID]
		,[IgnoreButKeepForReference]
		)
		SELECT
		{1} AS [ProjectID],
		tt1.[NameID],
		NULL,
		0 AS [IgnoreButKeepForReference]
		FROM [{2}] tt1
		WHERE tt1.[GBIFParentTaxonID] IS NULL
		 -- insert Hierarchy for synonyms too, should it just take over the parent taxa from GBIF or should they be calculated from JOIN with accepted taxon?
		AND tt1.[GBIFTaxonomicStatus] = 'accepted'
		;""".format(self.taxonhierarchytable, self.projectid, self.temptable)
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
		INSERT INTO [{0}] (
		[ProjectID]
		,[NameID]
		,[NameParentID]
		,[IgnoreButKeepForReference]
		)
		SELECT
		{1} AS [ProjectID],
		tt1.[NameID],
		NULL,
		0 AS [IgnoreButKeepForReference]
		FROM [{2}] tt1
		INNER JOIN [{2}] tt2 ON (tt1.[GBIFAcceptedTaxonID] = tt2.[GBIFTaxonID])
		WHERE tt2.[GBIFParentTaxonID] IS NULL
		
		AND tt1.[GBIFTaxonomicStatus] IN ('synonym', 'proparte synonym', 'heterotypic synonym', 'homotypic synonym')
		;""".format(self.taxonhierarchytable, self.projectid, self.temptable)
		self.cur.execute(query)
		self.con.commit()
		
		pass
		
		
		
		


