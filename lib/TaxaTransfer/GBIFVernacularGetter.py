

import logging
import logging.config

logger = logging.getLogger('sync_gbif2tnt')

import re
import pudb

from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')


from ..MySQLConnector import MySQLConnector
from .TaxaGetter import TaxaGetter


class GBIFVernacularGetter(TaxaGetter):
	def __init__(self, gbifdb):

		self.dbcon = MySQLConnector(gbifdb)
		TaxaGetter.__init__(self, self.dbcon)
		
		self.temptable = 'GBIFVernacularTempTable'
		self.gbifdb = gbifdb['db']
		self.vernaculartable = gbifdb['vernaculartable']
		
		
		self.createGBIFVernacularTempTable()
		
		self.setMaxPage()
	
	
	def getPageQuery(self):
		query = """SELECT 
		`TaxonID` AS `GBIFTaxonID`,
		`CommonName`,
		`LanguageCode`,
		`CountryCode`,
		`ReferenceTitle`
		FROM `{0}` WHERE `rownumber` BETWEEN %s AND %s""".format(self.temptable)
		return query
	

	def createGBIFVernacularTempTable(self):
		
		query = """CREATE TEMPORARY
		TABLE `{0}` 
		(rownumber INT(10) NOT NULL AUTO_INCREMENT,
		`TaxonID` INT(10) NOT NULL,
		`CommonName` VARCHAR(220) NOT NULL,
		`LanguageCode` VARCHAR(2) NOT NULL,
		`CountryCode` VARCHAR(10) NOT NULL,
		`ReferenceTitle` VARCHAR(220) NOT NULL,
		PRIMARY KEY (`rownumber`),
		KEY (`TaxonID`),
		KEY (`LanguageCode`)
		) CHARSET=utf8mb4
		;""".format(self.temptable)
		
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
		INSERT INTO `{0}`
		SELECT DISTINCT NULL, 
		`TaxonID`,
		SUBSTR(`vernacularName`, 1, 220) AS `CommonName`,
		`language` AS `LanguageCode`,
		`countryCode` AS `CountryCode`,
		SUBSTR(`source`, 1, 220) AS `ReferenceTitle`
		FROM `{1}`.`{2}` v
		WHERE v.`vernacularName` IS NOT NULL
		AND v.`vernacularName` != ''
		AND v.`language` IS NOT NULL
		AND v.`language` != ''
		AND v.`countryCode` IS NOT NULL
		AND v.`countryCode` != ''
		AND v.`source` IS NOT NULL
		AND v.`source` != ''
		 -- for test and development
		 -- LIMIT 100000
		;
		""".format(self.temptable, self.gbifdb, self.vernaculartable)
		
		self.cur.execute(query)
		self.con.commit()
		return

		
		
