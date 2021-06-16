import logging
import logging.config

logger = logging.getLogger('sync_gbif2tnt')

import pudb




class TaxaGetter():
	def __init__(self, dbconnector):
		self.dbcon = dbconnector
		
		self.con = self.dbcon.getConnection()
		self.cur = self.dbcon.getCursor()
		
		self.pagesize = 100 # do not set to a value where more than 2100 values where inserted
		# MSSQL Server appears to work with stored procedures when using the ? placeholders and pack them into parameters of the stored procedure 
		# There is a limit of 2100 parameters that can be used in a stored procedure
		# https://stackoverflow.com/questions/50212104/pyodbc-sqlalchemy-fails-for-more-than-2100-items
		# https://github.com/mkleehammer/pyodbc/issues/263
		self.page = 1
		self.taxanum = 0
		self.maxpage = 0
		self.pagingstarted = False
	
	
	def getCountQuery(self):
		query = """
		SELECT COUNT(*) FROM `{0}`
		;
		""".format(self.temptable)
		return query
	
	
	def setTaxaNum(self):
		query = self.getCountQuery()
		self.cur.execute(query)
		row = self.cur.fetchone()
		if row is not None:
			self.taxanum = row[0]
		else:
			self.taxanum = 0
	
	def getTaxaNum(self):
		self.setTaxaNum()
		return self.taxanum
	
	def setMaxPage(self):
		taxanum = self.getTaxaNum()
		if taxanum > 0:
			self.maxpage = int(taxanum / self.pagesize + 1)
		else:
			self.maxpage = 0
	
	def initPaging(self):
		self.currentpage = 1
		self.pagingstarted = True
	
	def getNextTaxaPage(self):
		if self.pagingstarted == False:
			self.initPaging()
		if self.currentpage <= self.maxpage:
			taxa = self.getTaxaPage(self.currentpage)
			self.currentpage = self.currentpage + 1
			#return taxa
			if len(taxa) > 0:
				return taxa
			else:
				self.pagingstarted = False
				return None
		else:
			self.pagingstarted = False
			return None
	
	
	def getTaxaPage(self, page):
		if page % 1000 == 0:
			#pudb.set_trace()
			logger.info("TaxaGetter get taxa page {0} of {1} pages".format(page, self.maxpage))
		startrow = ((page-1)*self.pagesize)+1
		lastrow = startrow + self.pagesize-1
		
		parameters = [startrow, lastrow]
		query = self.getPageQuery()
		
		self.cur.execute(query, parameters)
		taxa = self.cur.fetchall()
		return taxa
	
	
	def renameNomenClaturalCode(self):
		#TODO: the values in TaxonNameNomenclaturalCode_Enum differ between database instances, so this is really bad here
		
		
		query = """
		UPDATE `{0}` 
		set `nomenclaturalCode` = '3'
		WHERE `nomenclaturalCode` = 'Animalia'
		;
		""".format(self.temptable)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `nomenclaturalCode` = '2'
		WHERE `nomenclaturalCode` = 'Plantae' OR `nomenclaturalCode` = 'Fungi'
		;
		""".format(self.temptable)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `nomenclaturalCode` = '2'
		WHERE `nomenclaturalCode` IN ('Archaea', 'Bacteria', 'Chromista', 'Protozoa', 'Viruses', 'incertae sedis')
		;
		""".format(self.temptable)
		self.cur.execute(query)
		self.con.commit()
	

	def renameRanks(self):
		logger.info("GBIFTaxaGetter rename ranks")
		
		query = """
		UPDATE `{0}` 
		set `taxonRank` = 'sp.'
		WHERE `taxonRank` = 'species'
		;
		""".format(self.temptable)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `taxonRank` = 'var.'
		WHERE `taxonRank` = 'variety'
		;
		""".format(self.temptable)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `taxonRank` = 'subsp.'
		WHERE `taxonRank` = 'subspecies'
		;
		""".format(self.temptable)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `taxonRank` = 'phyl./div.'
		WHERE `taxonRank` = 'phylum'
		;
		""".format(self.temptable)
		self.cur.execute(query)
		self.con.commit()
		
		#query = """
		#UPDATE `{0}` 
		#set `taxonRank` = NULL
		#WHERE `taxonRank` = 'unranked'
		#;
		#""".format(self.temptable)
		#self.cur.execute(query)
		#self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `taxonRank` = 'fam.'
		WHERE `taxonRank` = 'family'
		;
		""".format(self.temptable)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `taxonRank` = 'ord.'
		WHERE `taxonRank` = 'order'
		;
		""".format(self.temptable)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `taxonRank` = 'cl.'
		WHERE `taxonRank` = 'class'
		;
		""".format(self.temptable)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `taxonRank` = 'f.'
		WHERE `taxonRank` = 'form'
		;
		""".format(self.temptable)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `taxonRank` = 'gen.'
		WHERE `taxonRank` = 'genus'
		;
		""".format(self.temptable)
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		UPDATE `{0}` 
		set `taxonRank` = 'reg.'
		WHERE `taxonRank` = 'kingdom'
		;
		""".format(self.temptable)
		self.cur.execute(query)
		self.con.commit()
		
	


	def dropTempTable(self):
		query = """DROP TABLE `{0}`;""".format(self.temptable)
		self.cur.execute(query)
		self.con.commit()
		return
	
