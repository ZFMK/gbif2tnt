import logging
import logging.config

logger = logging.getLogger('sync_gbif2tnt')
log_query = logging.getLogger('query')


import pudb

from .MSSQLConnector import MSSQLConnector




class DeleteTables():
	def __init__(self, tnt_con):
		'''
		this class completely removes the content of project given in self.projectname
		'''
		#pudb.set_trace()
		
		self.projectname = 'GBIF_Backbone_Taxonomy'
		
		self.tntdbcon = MSSQLConnector(config = tnt_con)
		
		self.con = self.tntdbcon.getConnection()
		self.cur = self.tntdbcon.getCursor()
		
		self.pagesize = 100000
		self.maxpage = 0
		
		
		self.filled_tables = [
			['TaxonHierarchy', 'NameID'],
			#['TaxonHierarchy_log', 'NameID'],
			['TaxonAcceptedName', 'NameID'],
			#['TaxonAcceptedName_log', 'NameID'],
			['TaxonSynonymy', 'NameID'],
			# ['TaxonSynonymy_log', 'NameID'],
			['TaxonNameExternalID', 'NameID'],
			# ['TaxonNameExternalID_log', 'NameID'],
			['TaxonCommonName', 'NameID'],
			# ['TaxonCommonName_log', 'NameID'],
			['TaxonName', 'NameID'],
			# ['TaxonName_log', 'NameID'],
			['TaxonNameProject', 'NameID'],
			# ['TaxonNameProject_log', 'NameID']
		]
		self.deleteTables()
	
	
	def drop_delete_table(self):
		query = """
		DROP TABLE IF EXISTS [NameIDs_to_delete];
		;"""
		log_query.info(query)
		self.cur.execute(query)
		self.con.commit()
		return
	
	
	def deleteTables(self):
		#pudb.set_trace()
		for table, idcolumn in self.filled_tables:
			logger.info("Deleting from table: {0}".format(table))
			self.setMaxPage(table, idcolumn)
			page = 1
			self.drop_delete_table()
			while page <= self.maxpage:
				self.deleteTablePage(table, idcolumn, page)
				page += 1
				self.drop_delete_table()
		return
	
	def deleteTablePage(self, table, idcolumn, page):
		#pudb.set_trace()
		if page % 10 == 0:
			logger.info("DeleteTables table: {0}, page: {1}".format(table, page))
		startid = ((page-1)*self.pagesize)+1
		lastid = startid + self.pagesize-1
		
		# temporary tables never work with placeholders in ms sql-server
		query = """
		SELECT dt.NameID 
		INTO [NameIDs_to_delete]
		FROM [{0}] dt
			INNER JOIN [TaxonNameProject] tnp1 ON (dt.NameID = tnp1.NameID)
			INNER JOIN [ProjectProxy] pp ON (tnp1.ProjectID = pp.ProjectID)
			WHERE pp.[Project] = ?
			AND (dt.[NameID] BETWEEN ? AND ?)
		""".format(table)
		
		log_query.info(query)
		logger.info('selecting into temporary table from table {0} where NameID between {1} and {2}'.format(table, startid, lastid))
		self.cur.execute(query, [self.projectname, startid, lastid])
		self.con.commit()
		
		# remove the NameIDs from temp table that are in other projects, too
		query = """
		DELETE t FROM [NameIDs_to_delete] t
		INNER JOIN [TaxonNameProject] tnp ON (tnp.NameID = t.NameID)
		INNER JOIN [ProjectProxy] pp ON (tnp.ProjectID = pp.ProjectID)
		WHERE pp.[Project] != ?
		"""
		log_query.info(query)
		self.cur.execute(query, [self.projectname])
		self.con.commit()
		
		query = """
		DELETE dt FROM [{0}] dt
		INNER JOIN [NameIDs_to_delete] t ON (dt.NameID = t.NameID)
		;""".format(table)
		
		log_query.info(query)
		self.cur.execute(query)
		self.con.commit()
		return
	
	
	def getMaxID(self, targettable, idcolumn):
		query = """
		SELECT ISNULL(MAX([{0}]), 0) FROM [{1}]
		;""".format(idcolumn, targettable)
		self.cur.execute(query)
		row = self.cur.fetchone()
		maxid = row[0]
		return maxid
	
	
	def setMaxPage(self, table, idcolumn):
		maxid = self.getMaxID(table, idcolumn)
		if maxid > 0:
			self.maxpage = int(maxid / self.pagesize + 1)
		else:
			self.maxpage = 0






