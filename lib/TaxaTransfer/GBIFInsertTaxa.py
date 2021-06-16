import logging
import logging.config

logger = logging.getLogger('sync_gbif2tnt')


import re
import pudb


# from ..MSSQLConnector import MSSQLConnector
from .GBIFInsertTNTTempTable import GBIFInsertTNTTempTableBase
from .InsertTaxonName import InsertTaxonNameBase
from .InsertTaxonNameProject import InsertTaxonNameProjectBase

from .InsertExternalDatabase import InsertExternalDatabaseBase
from .InsertTaxonNameListReference import InsertTaxonNameListReference
from .InsertTaxonHierarchy import InsertTaxonHierarchyBase
from .InsertTaxonSynonymy import InsertTaxonSynonymyBase
from .InsertTaxonAcceptedName import InsertTaxonAcceptedNameBase


class GBIFInsertTaxa():
	def __init__(self, tnt_con, taxagetter, tnt_projectid):
		self.taxagetter = taxagetter
		self.projectid = tnt_projectid
		
		
		self.tntdbcon = tnt_con
		
		self.insertTaxa()
		self.setTaxonHierarchy()
		self.setTaxonSynonymy()
		self.setTaxonAcceptedName()
	
	
	
	def insertTaxa(self):
		temptableimporter = GBIFInsertTNTTempTableBase(self.tntdbcon, self.taxagetter)
		self.temptablename = temptableimporter.getTempTableName()
		
		taxonnameimporter = InsertTaxonNameBase(self.tntdbcon, self.temptablename)
		
		taxonprojectimporter = InsertTaxonNameProjectBase(self.tntdbcon, self.temptablename, self.projectid)
		
		externaldatabaseimporter = InsertExternalDatabaseBase(self.tntdbcon, self.temptablename)
	
	
	def setTaxonHierarchy(self):
		taxonhierarchyimporter = InsertTaxonHierarchyBase(self.tntdbcon, self.temptablename, self.projectid)
		
	
	def setTaxonSynonymy(self):
		taxonsynonymyimporter = InsertTaxonSynonymyBase(self.tntdbcon, self.temptablename, self.projectid)
	
	def setTaxonAcceptedName(self):
		taxonacceptednameimporter = InsertTaxonAcceptedNameBase(self.tntdbcon, self.temptablename, self.projectid)
	
	
	def getTNTCon(self):
		return self.tntdbcon
	
	def getTempTable(self):
		return self.temptablename

	



