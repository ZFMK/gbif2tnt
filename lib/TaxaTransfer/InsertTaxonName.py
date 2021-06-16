import logging
import logging.config

logger = logging.getLogger('sync_gbif2tnt')
log_query = logging.getLogger('query')

import pudb


from .InsertIntoTablesBase import InsertIntoTablesBase

class InsertTaxonNameBase(InsertIntoTablesBase):
	def __init__(self, tntdbcon, temptablename):
		InsertIntoTablesBase.__init__(self, tntdbcon)
		
		self.temptable = temptablename
		
		
		self.taxonnametable = 'TaxonName'
		self.idcolumn = 'NameID'
		
		#pudb.set_trace()
		self.copyTaxonName()
	
	def copyTaxonName(self):
		
		logger.info("InsertTaxonName: inserting taxon names")
		maxid = self.getMaxID(self.taxonnametable, self.idcolumn)
		
		query = """
		INSERT INTO [{0}] (
		[NameID]
		,[TaxonomicRank]
		,[GenusOrSupragenericName]
		,[InfragenericEpithet]
		,[SpeciesEpithet]
		,[InfraspecificEpithet]
		,[BasionymAuthors]
		,[BasionymAuthorsYear]
		,[YearOfPubl]
		,[IsRecombination]
		,[IsHybrid]
		,[NomenclaturalCode]
		,[NomenclaturalStatus]
		)
		SELECT 
		[rownumber] + {1} AS [NameID]
		,[TaxonomicRank]
		,[GenusOrSupragenericName]
		,[InfragenericEpithet]
		,[SpeciesEpithet]
		,[InfraspecificEpithet]
		,[BasionymAuthors]
		,[BasionymAuthorsYear]
		,[YearOfPubl]
		,[IsRecombination]
		,[IsHybrid]
		,[NomenclaturalCode]
		,[NomenclaturalStatus]
		FROM [{2}]
		""".format(self.taxonnametable, maxid, self.temptable)
		
		log_query.info(query)
		
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		 -- set the NameID in temptable to the same value as the NameIDs in TaxonName
		UPDATE [{0}] 
		SET [NameID] = [rownumber] + {1}
		""".format(self.temptable, maxid)
		
		self.cur.execute(query)
		self.con.commit()
		
		pass
		
		
		
		


