# pigflux

Report statistics from postgresql database to influxdb. You can configure multiple postgresql and influxdb instances.

## Installation

The recommended way is to use pipenv to create a virtual environment:

    git clone git@github.com:nagylzs/pigflux.git
    cd pigflux
    pipenv install --skip-lock

Since pigflux is a single Python script, you can also install the required packages globally instead. Check `Pipenv`
file for required packages.

## Configuration

Copy `pigflux_example.yml` into `pigflux.yml`, and edit to your needs.

Main configuration sections:

* **databases** - named configurations for PostgreSQL instances
* **influxes** - named configurations for InfluxDb instances
* **tests** - named configurations for test queries

Each test can contain the following values:

* **databases** - a list of database configuration names. The test will be run on the given databases. (
  You can run the same test on multiple PostgreSQL databases)

* **influxes** - a list of influxdb configuration names. Test results will be sent to the given influxdb databases.

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

Use `--help` for command line options, including but not limited to:

## Run as a windows service

The easiest way to run pigflux is to use the [non-sucking service manager](https://nssm.cc/download).

* Download NSSM [from here](https://nssm.cc/download).
* Create a new service with:

    nssm.exe install pigflux
  
* Use the following settings:

  - Application path: point to pipenv.exe (e.g. "C:\Program Files\Python39\python.exe")
  - Startup directory: point to the directory containing pigflux.py and Pipenv
  - Arguments: `run python pigflux.py --count=-1 --silent --config=pigflux.yml` but pleae refer
    to `--help` for all options.
  - Change your display name, description, startup type as desired.
  - If you have used pipenv, then give your credentials on the "Log on" tab.
    Also, you need [to enable "service log on"](https://docs.microsoft.com/en-us/system-center/scsm/enable-service-log-on-sm?view=sc-sm-2019#enable-service-log-on-through-a-local-group-policy) for your user account.
  - Alternatively, if you have installed python and all required packages globally, then
    you can use local system account.
  - On the "Shutdown" tab, leave "Control+C" checked, but uncheck
    all others (WM_CLOSE, WM_QUIT, Terminate process)
  - Carefully check options on "Exit actions" tab. If you choose to use
    "restart application" then you should always set "deplay restart by"
    to a sensible value.
    
## TODO

There should be a way to merge tags from ancestor/template tests. The current implementation simply overwrites all tags.
Possible solution would be to add a new global `tags` section, and add a new `merge_tags` property to tests. 