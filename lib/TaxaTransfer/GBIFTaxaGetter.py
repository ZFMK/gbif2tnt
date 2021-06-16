

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


class GBIFTaxaGetter(TaxaGetter):
	def __init__(self, gbifdb):

		self.dbcon = MySQLConnector(gbifdb)
		TaxaGetter.__init__(self, self.dbcon)
		
		self.temptable = 'GBIFTempTable'
		self.gbifdb = gbifdb['db']
		self.taxontable = gbifdb['taxontable']
		
		kingdoms = config.get('transfer', 'restrict_kingdoms')
		if kingdoms == '':
			self.kingdoms = "'Animalia', 'Archaea', 'Bacteria', 'Chromista', 'Plantae', 'Fungi', 'Protozoa', 'Viruses', 'incertae sedis'"
		else:
			kingdomslist = kingdoms.split(',')
			self.kingdoms = "'" + "', '".join(kingdomslist) + "'"
		
		self.createGBIFTaxaTempTable()
		self.renameRanks()
		self.renameNomenClaturalCode()
		
		self.setMaxPage()
	
	
	def getPageQuery(self):
		query = """SELECT 
		`taxonRank` AS TaxonomicRank,
		`genericName` AS GenusOrSupragenericName,
		NULL AS InfragenericEpithet,
		`specificEpithet` AS SpeciesEpithet,
		`infraspecificEpithet` AS InfraspecificEpithet,
		NULL AS `Authors`,
		NULL AS `BasionymAuthorsYear`,
		NULL AS `PublicationYear`,
		0 AS IsRecombination,
		0 AS IsHybrid,
		`nomenclaturalCode` AS NomenclaturalCode,
		`taxonomicStatus` AS NomenclaturalStatus,
		`scientificName`,
		`scientificNameAuthorShip`,
		`TaxonID` AS GBIFTaxonID,
		CONCAT ('https://www.gbif.org/species/', TaxonID) AS GBIFTaxonURL,
		`GBIFDatasetID`,
		`GBIFParentTaxonID`,
		`GBIFAcceptedTaxonID`,
		`GBIFTaxonomicStatus`
		FROM `{0}` WHERE `rownumber` BETWEEN %s AND %s""".format(self.temptable)
		return query
	

	def createGBIFTaxaTempTable(self):
		
		query = """CREATE TEMPORARY
		TABLE `{0}` 
		(rownumber INT(10) NOT NULL AUTO_INCREMENT,
		`TaxonID` INT(10) NOT NULL,
		`GBIFDatasetID` VARCHAR(50) NOT NULL,
		`GBIFParentTaxonID` INT(10) DEFAULT NULL,
		`GBIFAcceptedTaxonID` INT(10) DEFAULT NULL,
		`GBIFTaxonomicStatus` VARCHAR(255),
		`canonicalName` VARCHAR(255),
		`parentCanonicalName` VARCHAR(255),
		`scientificName` VARCHAR(255),
		`scientificNameAuthorShip` VARCHAR(255),
		`genericName` VARCHAR(255),
		`specificEpithet` VARCHAR(255),
		`infraspecificEpithet` VARCHAR(255),
		`taxonRank` VARCHAR(50),
		`taxonomicStatus` VARCHAR(50),
		`nomenclaturalCode` VARCHAR(50),
		PRIMARY KEY (`rownumber`),
		KEY (`taxonRank`)
		) CHARSET=utf8mb4
		;""".format(self.temptable)
		
		self.cur.execute(query)
		self.con.commit()
		
		
		query = """
		INSERT INTO `{0}`
		SELECT DISTINCT NULL, 
		`TaxonID`,
		`datasetID` AS `GBIFDatasetID`,
		`parentNameUsageID` AS `GBIFParentTaxonID`,
		`acceptedNameUsageID` AS `GBIFAcceptedTaxonID`,
		`taxonomicStatus` AS `GBIFTaxonomicStatus`,
		`canonicalName`,
		`parentCanonicalName`,
		`scientificName`,
		`scientificNameAuthorShip`,
		`genericName`,
		`specificEpithet`,
		`infraspecificEpithet`,
		`taxonRank`,
		NULL AS `taxonomicStatus`,
		`kingdom` AS `nomenclaturalCode`
		FROM `{1}`.`{2}` t 
		WHERE t.taxonomicStatus IN ('accepted', 'synonym', 'proparte synonym', 'heterotypic synonym', 'homotypic synonym')
		AND (t.canonicalName IS NOT NULL AND t.canonicalName != "") 
		 -- AND (t.parentCanonicalName IS NOT NULL AND t.parentCanonicalName != "")
		AND t.taxonRank IS NOT NULL
		AND t.taxonRank != 'unranked'
		AND `kingdom` IN ({3})
		OR t.canonicalName IN ({3})
		 -- for test and development
		 -- LIMIT 100000
		;
		""".format(self.temptable, self.gbifdb, self.taxontable, self.kingdoms)
		
		self.cur.execute(query)
		self.con.commit()
		return

		
		
