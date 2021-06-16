#!/usr/bin/env python
# -*- coding: utf-8 -*-



import pudb
import datetime


from lib.SetProject import SetProject
from lib.DeleteTables import DeleteTables


from lib.MSSQLConnector import MSSQLConnector

from lib.TaxaTransfer.GBIFTaxaGetter import GBIFTaxaGetter
from lib.TaxaTransfer.GBIFVernacularGetter import GBIFVernacularGetter
from lib.TaxaTransfer.GBIFInsertTaxa import GBIFInsertTaxa
from lib.TaxaTransfer.InsertCommonNames import InsertCommonNames
from lib.ClosureTable.TaxaClosureTable import TaxaClosureTable



import pudb


from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')


import logging, logging.config
if __name__ == "__main__":
	logging.config.fileConfig('config.ini', defaults={'logfilename': 'sync_gbif2tnt.log'}, disable_existing_loggers=False)
logger = logging.getLogger('sync_gbif2tnt')



if __name__ == "__main__":
	logger.info("\n\n======= S t a r t - {:%Y-%m-%d %H:%M:%S} ======".format(datetime.datetime.now()))
	
	# copy the connection data into dict
	gbifdb_config = dict(config['gbifdb_con'])
	
	tnt_config = dict(config['tnt_con'])
	
	tntdbcon = MSSQLConnector(config = tnt_config)
	
	testing = config.getboolean('transfer', 'test')
	# for testing
	if testing is True:
		deletetables = DeleteTables(tnt_config)
	
	
	project = SetProject(tnt_config)
	tnt_projectid = project.getProjectID()
	
	
	gbiftaxagetter = GBIFTaxaGetter(gbifdb_config)
	
	
	#pudb.set_trace()
	taxaimporter = GBIFInsertTaxa(tntdbcon, gbiftaxagetter, tnt_projectid)
	taxonnametemptable = taxaimporter.getTempTable()
	
	gbifvernaculargetter = GBIFVernacularGetter(gbifdb_config)
	commonnameimporter = InsertCommonNames(tntdbcon, taxonnametemptable, gbifvernaculargetter)
	
	closuretable = TaxaClosureTable(tntdbcon, tnt_projectid)
	closuretable.createClosureTable()
	closuretable.fillClosureTable()


	logger.info("\n======= E N D - {:%Y-%m-%d %H:%M:%S} ======\n\n".format(datetime.datetime.now()))


