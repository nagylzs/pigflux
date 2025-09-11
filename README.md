# pigflux

Report statistics from postgresql/mysql database to influxdb and/or postgresql. 
You can configure multiple postgresql/mysql and influxdb instances.

## Build

TODO

## Configuration

Copy `pigflux_example.yml` into `pigflux.yml`, and edit to your needs.

Main configuration sections:

* **databases** - named configurations for PostgreSQL instances
* **influxes** - named configurations for InfluxDb v1 instances
* **influxes2** - named configurations for InfluxDb v2 instances
* **tests** - named configurations for test queries

Each test can contain the following values:

* **databases** - a list of database configuration names. The test will be run on the given databases. (
  You can run the same test on multiple PostgreSQL databases)

* **influxes** - a list of influxdb (v1 or v2) configuration names. Test results will be sent to the given influxdb databases.

* **measurement** - destination measurement name for the test
* **tags** - an object (key-value pairs) that will be used for tagging the measurement. Please note that InfluxDb
  supports string tag values only. The name of the database will be added as an extra tag called `database`. 
* **sql** - an SQL SELECT command that will be used to fetch measurement data from the PostgreSQL database
* **fields** - a list of field names, they will be used to access measurement values from the fetched data.
* **order** - a number that will be used to determine the order of execution. When not given, it defaults to 100.
* **is_template** - When set, this test will not be executed, but it can be used as a template.

* **inherit_from** - name of another test that will be used to inherit almost all properties from. The is_template and
  inherit_from properties cannot be inherited.

## Run

You can start pigflux from command line or cron:

  pipenv run python pigflux.py -c pigflux.yml

Use `--help` for command line options.

## Run as a windows service

The easiest way to run pigflux is to use the [non-sucking service manager](https://nssm.cc/download).

* Download NSSM [from here](https://nssm.cc/download).
* Create a new service with:

    nssm.exe install pigflux
  
* Use the following settings: TODO
    
## TODO

There should be a way to merge tags from ancestor/template tests. The current implementation simply overwrites all tags.
Possible solution would be to add a new global `tags` section, and add a new `merge_tags` property to tests. 