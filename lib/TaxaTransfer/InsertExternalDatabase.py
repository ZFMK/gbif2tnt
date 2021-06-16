import logging
import logging.config

logger = logging.getLogger('sync_gbif2tnt')


import re
import pudb

from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')


from .InsertIntoTablesBase import InsertIntoTablesBase

class InsertExternalDatabaseBase(InsertIntoTablesBase):
	def __init__(self, tntdbcon, temptablename):
		InsertIntoTablesBase.__init__(self, tntdbcon)
		
		self.temptable = temptablename
		
		self.externaldatabasetable = 'TaxonNameExternalDatabase'
		self.idcolumn = 'ExternalDatabaseID'
		
		self.externalidstable = 'TaxonNameExternalID'
		
		self.gbif_source = dict(config['gbif_source_details'])
		self.edb_uri = self.gbif_source['uri']
		self.edb_name = self.gbif_source['name']
		self.edb_accession_date = self.gbif_source['accessiondate']
		self.edb_version = self.gbif_source['version']
		self.edb_license = self.gbif_source['license']
		
		
		self.insertExternalDatabase()
		self.insertExternalIDs()
		
		
		
	
	def getExternalDatabaseID(self):
		query = """
		SELECT [ExternalDatabaseID]
		FROM 
		[{0}]
		WHERE
		[ExternalDatabaseURI] = ?
		AND [ExternalDatabaseName] = ?
		AND [ExternalDatabaseVersion] = ?
		""".format(self.externaldatabasetable)
		
		self.cur.execute(query, [
			self.edb_uri, 
			self.edb_name, 
			self.edb_version
		])
		row = self.cur.fetchone()
		if row is None:
			return None
		else:
			return row[0] 
		
		
		
	
	
		
	
	def insertExternalDatabase(self):
		self.databaseid = self.getExternalDatabaseID()
		if self.databaseid is not None:
			return
		else:
			maxid = self.getMaxID(self.externaldatabasetable, self.idcolumn)
			query = """
			INSERT INTO [{0}] (
				[ExternalDatabaseID]
				, [ExternalDatabaseURI]
				, [ExternalDatabaseName]
				, [InternalNotes]
				, [ExternalDatabaseVersion]
				, [Rights]
			)
			VALUES (?, ?, ?, ?, ?, ?)
			;""".format(self.externaldatabasetable)
			self.cur.execute(query, [
				maxid + 1,
				self.edb_uri, 
				self.edb_name, 
				self.edb_accession_date, 
				self.edb_version, 
				self.edb_license
			])
			self.con.commit()
			self.databaseid = maxid + 1
			return
		
		
		
	
	def insertExternalIDs(self):
		
		query = """
		INSERT INTO [{0}]
		(
			[NameID],
			[ExternalDatabaseID],
			[ExternalNameURI]
		)
		SELECT [NameID] 
		, {1} AS [ExternalDatabaseID]
		, [GBIFTaxonURL]
		FROM
		[{2}]
		;""".format(self.externalidstable, self.databaseid, self.temptable)
		
		self.cur.execute(query)
		self.con.commit()
		
		
		
	
	
		
		


