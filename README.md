# gbif2tnt
Import GBIF Backbone Taxonomy from intermediate MySQL Database into an DiversityTaxonNames instance

## Prerequisites

1. The gbif2tnt script requires that a MySQL database exists that have been created with the [gbif2mysql](#gbif2mysql) script
2. A running instance of [DiversityTaxonNames](https://diversityworkbench.net/Portal/DiversityTaxonNames) as well as access to its database on a MS SQL Server is required
3. An installation of FreeTDS for connecting to the TaxonNames database (see [FreeTDS]() below)






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



