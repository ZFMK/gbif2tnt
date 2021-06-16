#!/usr/bin/env python
# -*- coding: utf-8 -*-



import pudb
import datetime

from lib.MSSQLConnector import MSSQLConnector

from lib.SetProject import SetProject
from lib.ClosureTable.TaxaClosureTable import TaxaClosureTable


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
	
	#pudb.set_trace()
	
	project = SetProject(tnt_config)
	tnt_projectid = project.getProjectID()
	
	
	closuretable = TaxaClosureTable(tntdbcon, tnt_projectid)
	closuretable.createClosureTable()
	closuretable.fillClosureTable()
	


	logger.info("\n======= E N D - {:%Y-%m-%d %H:%M:%S} ======\n\n".format(datetime.datetime.now()))


