
import logging
import logging.config

logger = logging.getLogger('sync_gbif2tnt')
log_queries = logging.getLogger('query')


import pudb



class TaxaClosureTable():
	def __init__(self, dbcon, projectid):
		#pudb.set_trace()
		self.closuretable = 'TaxonNameClosureTable'
		self.taxonnametable = 'TaxonName'
		self.projectid = projectid
		self.con = dbcon.getConnection()
		self.cur = dbcon.getCursor()
		self.dbname = dbcon.getDatabaseName()
		
		
	
	def createClosureTable(self):
		query = """IF OBJECT_ID('{0}.dbo.{1}', 'U') IS NOT NULL DROP TABLE {0}.dbo.[{1}];""".format(self.dbname, self.closuretable)
		
		self.cur.execute(query)
		self.con.commit()
		
		query = """CREATE TABLE [{0}] (
		[AncestorID] INT NOT NULL, -- NameID calculated from NameID and SourceParentID relations
		[DescendantID] INT NOT NULL, -- NameID calculated from NameID and SourceParentTaxonID relations
		[PathLength] INT,
		[ProjectID] INT DEFAULT NULL,
		INDEX idx_AncestorID ([AncestorID]),
		INDEX idx_DescendantID ([DescendantID]),
		INDEX idx_PathLength ([PathLength]),
		INDEX idx_ProjectID ([ProjectID])
		)
		;""".format(self.closuretable)
		
		self.cur.execute(query)
		self.con.commit()
	
	def getNextLevelCount(self, pathlength):
		query = """
		SELECT COUNT(*)
		FROM [{0}] tr
		WHERE tr.pathLength = ?
		""".format(self.closuretable)
		
		self.cur.execute(query, [pathlength])
		row = self.cur.fetchone()
		if row is not None:
			count = row[0]
		else:
			count = 0
		return count
	
	
	def fillClosureTable(self):
		logger.info("TaxaClosureTable: Fill closure table")
		
		#pudb.set_trace()
		# set the relation to it self for each taxon entry
		query = """
		INSERT INTO [{0}] ([AncestorID], [DescendantID], [PathLength], [ProjectID])
		SELECT [NameID], [NameID], 0, {1} AS [ProjectID] FROM [TaxonName];
		""".format(self.closuretable, int(self.projectid))
		self.cur.execute(query)
		self.con.commit()
		
		
		pathlength = 0
		levelcount = self.getNextLevelCount(pathlength)
		
		
		while levelcount > 0:
			#pudb.set_trace()
			# set the parent relations
			logger.info("TaxaClosureTable: Fill closure table with pathlength {0}, with {1} possible childs".format(pathlength, levelcount))
			query = """
			INSERT INTO [{0}] ([AncestorID], [DescendantID], [PathLength], [ProjectID])
			SELECT th.[NameParentID], tr.[DescendantID], tr.pathLength + 1, tr.[ProjectID]
			FROM [{0}] tr
			INNER JOIN [TaxonHierarchy] th
			ON (
				th.[NameID] = tr.[AncestorID]
				AND th.[ProjectID] = tr.[ProjectID]
			)
			 -- ensure that there is a taxon name connected to NameParentID
			INNER JOIN [TaxonName] tn
			ON (
				tn.[NameID] = th.[NameParentID]
			)
			WHERE 
			th.NameID != th.NameParentID
			AND tr.pathLength = ?
			AND tr.ProjectID = ?
			""".format(self.closuretable)
			self.cur.execute(query, [pathlength, self.projectid])
			self.con.commit()
			
			pathlength += 1
			levelcount = self.getNextLevelCount(pathlength)
		
		return
	
	
	
	
		
