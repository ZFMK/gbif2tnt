import logging
import logging.config

logger = logging.getLogger('sync_gbif2tnt')

import pudb


from .MSSQLConnector import MSSQLConnector


class SetProject():
	def __init__(self, tnt_con):
		self.tntdbcon = MSSQLConnector(config = tnt_con)
		
		self.con = self.tntdbcon.getConnection()
		self.cur = self.tntdbcon.getCursor()
		
		self.project = {
			"name": 'GBIF_Backbone_Taxonomy',
			"projectid": None
		}
		
		projectid = self.getProjectID()
		if projectid is None:
			self.insertProject()
	
	
	def getProjectID(self):
		query = """SELECT ProjectID FROM [ProjectProxy]
		WHERE Project = ?
		;"""
		self.cur.execute(query, [self.project['name']])
		row = self.cur.fetchone()
		if row is None:
			return None
		else:
			return row[0]

	def insertProject(self):
		query = """SELECT MAX(ProjectID) FROM [ProjectProxy]
		;"""
		self.cur.execute(query)
		row = self.cur.fetchone()
		
		if row is not None:
			self.project['projectid'] = int(row[0]) + 1
		else:
			self.project['projectid'] = 1
		
		
		
		query = """INSERT INTO [ProjectProxy] 
		([ProjectID], [Project])
		VALUES (?, ?)
		;"""
		
		self.cur.execute(query, [self.project['projectid'], self.project['name']])
		self.con.commit()
		
		
		
		

 
