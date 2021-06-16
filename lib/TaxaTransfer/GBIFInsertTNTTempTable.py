import logging
import logging.config

logger = logging.getLogger('query')


from .InsertIntoTablesBase import InsertIntoTablesBase

import re
import pudb


class GBIFInsertTNTTempTableBase(InsertIntoTablesBase):
	def __init__(self, tntdbcon, taxagetter):
		InsertIntoTablesBase.__init__(self, tntdbcon)
		
		self.taxagetter = taxagetter
		
		# matchobject.groups() = [Taxon, Subgenus with parenthesis, Subgenus without parenthesis, Speciesepithet, SubspeciesEpithet, Authorship(incl. year), Authors, Year]
		# e. g.
		# 'Erpobdella (Nephelis) octoculata bla (Linne & Testman, 1758)'
		# -> ('Erpobdella', '(Nephelis)', 'Nephelis', 'octoculata', 'bla', '(Linne & Testman, 1758)', 'Linne & Testman', '1758')
		# 'Erpobdella octoculata (Linnaeus, 1758)'
		# -> ('Erpobdella', None, None, 'octoculata', None, '(Linnaeus, 1758)', 'Linnaeus', '1758')
		#self.taxonpattern = re.compile('([A-Z][a-z|-]+)\s*(\(([^\,]*?)\))*\s*([a-z|-]+)*\s*([a-z|-]+)*\s*([\(]*([A-Za-z].*)\,\s*(\d{2,4}).*)*')
		
		self.namepattern = re.compile(r'([A-Z][a-z|-]+)\s*(\(([^\,]*?)\))*\s*([a-z|-]+)*\s*([a-z|-]+)*')
		self.authorpattern = re.compile(r'([\(]?([A-Za-z][^\)\d]*[a-z])[\,\s]*(\d{2,4})?[\)]?)')
		
		self.temptable = '#TempTaxonName'
		
		self.taxa = []
		self.convertedtaxa = []
		self.createTaxonNameTempTable()
		self.fillTaxaTempTable()
	
	
	
	def prepareTaxonNameData(self):
		self.convertedtaxa = []
		for taxarow in self.taxa:
			row = list(taxarow)
			
			'''
			[TaxonomicRank] -- 0 done
			,[GenusOrSupragenericName] -- 1 done
			,[InfragenericEpithet] -- 2 pattern?
			,[SpeciesEpithet] -- 3 done
			,[InfraspecificEpithet] -- 4 done
			,[BasionymAuthors] -- 5 pattern
			,[BasionymAuthorsYear] -- 6 pattern
			,[YearOfPubl] -- 7 pattern
			,[IsRecombination] -- 8 pattern
			,[IsHybrid] -- 9 not used
			,[NomenclaturalCode] -- 10 done 
			,[NomenclaturalStatus] -- 11 NULL done
			,[GBIFScientificName] -- 12 done
			,[GBIFAuthorShip] -- 13 
			,[GBIFTaxonID]
			,[GBIFTaxonURL]
			,[GBIFdatasetID]
			,[GBIFParentTaxonID]
			,[GBIFAcceptedTaxonID]
			,[GBIFTaxonomicStatus]
			,scientificAuthorShip -- 
			'''
			
			
			# genusorsupragenericname = row[1]
			scientificname = row[12]
			authorship = row[13]
			
			
			# [InfragenericEpithet], [IsRecombination]
			if scientificname is not None:
				m = self.namepattern.search(scientificname)
				if m is not None:
					# subgenus
					if m.groups()[2] is not None:
						row[2] = m.groups()[2]
			
			
			if (authorship is not None) and (authorship != ''):
				m = self.authorpattern.search(authorship)
				# Authors
				if m is None:
					#pudb.set_trace()
					pass
				else:
					if m.groups()[1] is not None:
						if len(m.groups()[1]) < 100:
							row[5] = m.groups()[1]
						else:
							row[5] = m.groups()[1]
							authors = m.groups()[1].split(',')
							authorstring = ', '.join(authors[:3]) + ' et al.'
							if len(authorstring) > 99:
								# when the string is still to long
								authorstring = authorstring[:99]
							row[5] = authorstring
					#Year (BasionymAuthorsYear and YearOfPublication)
					
					# IsRecombination
					if m.groups()[1] is not None:
						#pudb.set_trace()
						if m.groups()[0].startswith('('):
							row[8] = 1
							#do not use it for animals
							#row[10] = 'comb.'
					
					if m.groups()[2] is not None:
						row[6] = m.groups()[2]
						row[7] = m.groups()[2]
			self.convertedtaxa.append(row)
				
		
		
		
	def createTaxonNameTempTable(self):
		query = """SELECT TOP 0
			IDENTITY (INT) as rownumber, -- set an IDENTITY column that can be used for paging
			tn1.[TaxonomicRank]
			,tn1.[GenusOrSupragenericName]
			,tn1.[InfragenericEpithet]
			,tn1.[SpeciesEpithet]
			,tn1.[InfraspecificEpithet]
			,tn1.[BasionymAuthors]
			,tn1.[BasionymAuthorsYear]
			,tn1.[YearOfPubl]
			,tn1.[IsRecombination]
			,tn1.[IsHybrid]
			,tn1.[NomenclaturalCode]
			,tn1.[NomenclaturalStatus]
			INTO [{0}]
			FROM [TaxonName] tn1
			;""".format(self.temptable)
		
		logger.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
			ALTER TABLE [{0}]
			ADD [NameID] INT
			, [GBIFScientificName] VARCHAR(255)
			, [GBIFAuthorShip] VARCHAR(255)
			, [GBIFTaxonID] INT
			, GBIFTaxonURL VARCHAR(255)
			, [GBIFDatasetID] VARCHAR(100)
			, [GBIFParentTaxonID] INT
			, [GBIFAcceptedTaxonID] INT
			, [GBIFTaxonomicStatus] VARCHAR(255)
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
		
		query = """
			CREATE INDEX [GBIFParentTaxonID_idx] ON [{0}] ([GBIFParentTaxonID])
			;""".format(self.temptable)
		logger.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
			CREATE INDEX [GBIFAcceptedTaxonID_idx] ON [{0}] ([GBIFAcceptedTaxonID])
			;""".format(self.temptable)
		logger.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
			CREATE INDEX [GBIFTaxonomicStatus_idx] ON [{0}] ([GBIFTaxonomicStatus])
			;""".format(self.temptable)
		logger.info(query)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
			CREATE INDEX [NameID_idx] ON [{0}] ([NameID])
			;""".format(self.temptable)
		logger.info(query)
		self.cur.execute(query)
		self.con.commit()
	
	
	def fillTaxaTempTable(self):
		self.taxa = self.taxagetter.getNextTaxaPage()
		
		while self.taxa is not None:
			
			self.prepareTaxonNameData()
			
			self.setPlaceholderString(self.convertedtaxa)
			self.setValuesFromLists(self.convertedtaxa)
			
			query = """
			INSERT INTO [{0}]
			(
			[TaxonomicRank]
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
			,[GBIFScientificName]
			,[GBIFAuthorShip]
			,[GBIFTaxonID]
			,[GBIFTaxonURL]
			,[GBIFdatasetID]
			,[GBIFParentTaxonID]
			,[GBIFAcceptedTaxonID]
			,[GBIFTaxonomicStatus]
			)
			VALUES {1}
			;""".format(self.temptable, self.placeholderstring)
			
			
			logger.info(query)
			#logger.info(self.values)
			
			
			self.cur.execute(query, self.values)
			self.con.commit()
			
			self.taxa = self.taxagetter.getNextTaxaPage()
		
	
	
	
	
	def getTempTableName(self):
		return self.temptable
		
		



