# gbif2tnt
Import GBIF Backbone Taxonomy from intermediate MySQL Database into an DiversityTaxonNames instance

## Prerequisites

1. The gbif2tnt script requires that a MySQL database exists that have been created with the [gbif2mysql](https://github.com/ZFMK/gbif2mysql) script
2. A running instance of [DiversityTaxonNames](https://diversityworkbench.net/Portal/DiversityTaxonNames) as well as access to its database on a MS SQL Server is required
3. An installation of FreeTDS for connecting to the TaxonNames database (see [FreeTDS](https://github.com/ZFMK/gbif2tnt/tree/main#freetds) below)



## gbif2tnt installation

### Create Python Virtual Environment:


    python3 -m venv gbif2tnt_venv
    cd gbif2tnt_venv


Activate virtual environment:

    source bin/activate

Upgrade pip and setuptools

    python -m pip install -U pip
    pip install --upgrade pip setuptools

Clone gbif2mysql from github: 

    git clone https://github.com/ZFMK/gbif2tnt.git


Install the gbif2tnt script:

    cd gbif2tnt
    python setup.py develop

Create and edit the config file

    cp config.template.ini config.ini

Insert the needed database connection values:

    [gbifdb_con]
    # this is the MySQL database containing the imported GBIF Backbone data as flat tables `Taxon` and `VernacularName`
    host = 
    user = 
    passwd = 
    db = 
    charset = utf8
    taxontable = Taxon
    vernaculartable = VernacularName
    
    [tnt_con]
    # This is the DiversityTaxonNames database where the GBIF Backbone data should be imported
    DSN = 
    user = 
    passwd = 
    port = 
    db = 


Adapt the information on the download of the GBIF Backbone Taxonomy

    [gbif_source_details]
    # Information on the downloaded GBIF Backbone data
    uri = https://doi.org/10.15468/39omei
    name = GBIF Backbone Taxonomy
    accessiondate = 
    version = 
    license = CC BY 4.0


## Running gbif2tnt

    python Transfer.py

This script takes about 3.5 hours when existing datasets from a former GBIF Taxonomy import must be deleted first. Otherwise it takes about 1.5 hours. Progress is printed to terminal.


----

## FreeTDS

Download and install FreeTDS driver for SQL-Server Database

    wget ftp://ftp.freetds.org/pub/freetds/stable/freetds-1.2.18.tar.gz
    tar -xf freetds-1.2.18.tar.gz
    cd freetds-1.2.18
    ./configure --prefix=/usr --sysconfdir=/etc --with-unixodbc=/usr --with-tdsver=7.2
    make
    sudo make install

Setup odbc-driver and config

Create file `tds.driver.template` with content:

    [FreeTDS]
    Description = v0.82 with protocol v8.0
    Driver = /usr/lib/libtdsodbc.so


Register driver

    sudo odbcinst -i -d -f tds.driver.template

Create entry in `/etc/odbc.ini` 

    [TaxonNames] 
    Driver=FreeTDS
    TDS_Version=7.2
    APP=Some meaningful appname
    Description=DWB SQL DWB Server
    Server=<some TaxonNames Server>
    Port=<port>
    Database=<a TaxonNames database>
    QuotedId=Yes
    AnsiNPW=Yes
    Mars_Connection=No
    Trusted_Connection=Yes
    client charset = UTF-8



