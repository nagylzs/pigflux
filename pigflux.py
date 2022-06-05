import argparse
import os
import sys
import time
import traceback
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Type, List

import psycopg2
import mysql.connector
import yaml
import influxdb
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from yaml2dataclass import Schema, SchemaPath

drivers = {
    "postgres": psycopg2,
    "mysql": mysql.connector
}


@dataclass
class DatabaseConfiguration(Schema):
    driver: str
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass
class InfluxDbConfiguration(Schema):
    host: str
    port: int  # Common ports: 443
    ssl: bool
    verify_ssl: bool
    database: str
    username: str
    password: str

    def get_client_params(self):
        result = asdict(self)
        if "tags" in result:
            del result["tags"]
        return result


@dataclass
class InfluxDb2Configuration(Schema):
    url: str
    org: str
    bucket: str
    token: str

    def get_client_params(self):
        return dict(url=self.url, token=self.token)


@dataclass
class Test(Schema):
    name: str
    databases: Optional[List[str]] = None  # databases to run test on
    influxes: Optional[List[str]] = None  # influxes to send test results to
    measurement: Optional[str] = None  # name of the measurement
    tags: Optional[Dict[str, str]] = None  # a dict of tags to add to the points
    fields: Optional[List[str]] = None  # list of field names
    sql: Optional[str] = None  # SQL query to run to get field values
    order: Optional[int] = 100  # Order of tests
    is_template: Optional[bool] = False  # Set flag if this is a template
    inherit_from: Optional[str] = None  # Inherit properties from

    INHERIT_PROP_NAMES = ["databases", "influxes", "measurement", "tags", "fields", "sql", "order"]

    @staticmethod
    def bind_prop(prop_name, used: List[str]):
        for test_name in used:
            test = config.tests[test_name]
            value = getattr(test, prop_name)
            if value is not None:
                return value

    def check(self):
        if not self.is_template:
            for database in self.databases:
                if database not in config.databases:
                    error("%s: invalid database %s" % (self.name, database))
            for influx in self.influxes:
                if influx not in config.influxes:
                    error("%s: invalid influx %s" % (self.name, influx))


@dataclass
class AppConfiguration(Schema):
    databases: Dict[str, DatabaseConfiguration]
    influxes: Dict[str, InfluxDbConfiguration]
    influxes2: Dict[str, InfluxDb2Configuration]
    tests: Dict[str, Test]

    @classmethod
    def _load_dict(cls, props_dict, dest_cls: Type[Schema], add_name: bool = False):
        result = {}
        for name, value in props_dict.items():
            arguments = {}
            arguments.update(value)
            if add_name:
                arguments["name"] = name
            result[name] = dest_cls.scm_load_from_dict(arguments)
        return result

    @classmethod
    def scm_convert(cls, values: dict, path: SchemaPath):
        values["databases"] = cls._load_dict(values["databases"], DatabaseConfiguration)
        values["influxes"] = cls._load_dict(values["influxes"], InfluxDbConfiguration)
        values["influxes2"] = cls._load_dict(values["influxes2"], InfluxDb2Configuration)
        values["tests"] = cls._load_dict(values["tests"], Test, True)
        return values


def load_app_config(stream) -> AppConfiguration:
    """Load application configuration from a stream."""
    obj = yaml.safe_load(stream)
    return AppConfiguration.scm_load_from_dict(obj)


def error(message: str):
    sys.stderr.write("\nerror: " + message + "\n")
    sys.stderr.flush()
    raise SystemExit(-1)


def inherit_props(name: str, used: List[str]):
    test = config.tests[name]
    ref = test.inherit_from
    if ref:
        if ref in used:
            error("Circular reference tests.%s.inherit_from=%s (used=%s)" % (name, ref, used))
        if ref not in config.tests:
            error("Invalid reference tests.%s.inherit_from=%s" % (name, ref))
        used.append(name)
        inherit_props(ref, used)
        used.remove(name)

        for prop_name in test.INHERIT_PROP_NAMES:
            if getattr(test, prop_name) is None:
                inherit_from = config.tests[test.inherit_from]
                setattr(test, prop_name, getattr(inherit_from, prop_name))
            if not test.is_template and getattr(test, prop_name) is None:
                error("tests.%s.%s is required" % (name, prop_name))


def info(*values):
    if not args.silent:
        print(*values)


def main():
    # Test for circular references, inherit properties
    for name, value in config.tests.items():
        inherit_props(name, [])
    # Check database/influx references
    for name, value in config.tests.items():
        value.check()

    # Sort tests by their order number
    tests = sorted(config.tests.values(), key=lambda test: test.order)

    def connect(database_name):
        cfg = asdict(config.databases[database_name])
        if "driver" in cfg:
            driver_name = cfg["driver"]
            del cfg["driver"]
        else:
            driver_name = "postgres"
        try:
            driver = drivers[driver_name]
        except KeyError:
            raise ValueError("Unknown driver %s (valid drivers: %s)" % (driver_name, repr(list(drivers.keys()))))
        return driver.connect(**cfg)

    # Collect data
    points = {influx_name: [] for influx_name in config.influxes}
    points2 = {influx_name: [] for influx_name in config.influxes2}
    for test in tests:
        if not test.is_template:
            info("    Running test %s" % test.name)
            for database_name in test.databases:
                conn = connect(database_name)
                cur = conn.cursor()
                q_started = time.time()
                cur.execute(test.sql)
                row = cur.fetchone()
                q_elapsed = time.time() - q_started
                column_map = {desc[0]: idx for idx, desc in enumerate(cur.description)}
                fields = {field: row[column_map[field]] for field in test.fields}
                fields["q_elapsed"] = q_elapsed
                tags = {"database_name": database_name}
                tags.update(test.tags)
                point = dict(measurement=test.measurement, tags=tags, fields=fields)
                for influx_name in test.influxes:
                    if influx_name in points:
                        points[influx_name].append(point)
                    elif influx_name in points2:
                        points2[influx_name].append(point)
                    else:
                        raise KeyError("Invalid influx name: %s" % influx_name)

    for influx_name, influx in config.influxes.items():
        pts = points[influx_name]
        if pts:
            info("    Sending %d point(s) to influxdb v1 %s" % (len(pts), influx_name))
            try:
                influx = config.influxes[influx_name]
                client = influxdb.InfluxDBClient(influx.get_client_params())
                client.write_points(pts)
            except:
                if args.halt_on_send_error:
                    raise
                else:
                    traceback.print_exc(file=sys.stderr)

    for influx_name, influx in config.influxes2.items():
        pts = points2[influx_name]
        if pts:
            info("    Sending %d point(s) to influxdb v2 %s" % (len(pts), influx_name))
            try:
                influx2 = config.influxes2[influx_name]
                client = influxdb_client.InfluxDBClient(influx2.get_client_params())
                write_api = client.write_api(write_options=SYNCHRONOUS)
                write_api.write(bucket=influx2.bucket, org=influx2.bucket, record=pts)
            except:
                if args.halt_on_send_error:
                    raise
                else:
                    traceback.print_exc(file=sys.stderr)


parser = argparse.ArgumentParser(description='Execute PostgreSQL queries and send results into influxdb.')

parser.add_argument('-c', "--config", dest="config", default=None,
                    help="Configuration file for application. Default is pigflux.yml. "
                         "See pigflux_example.yml for an example.")
parser.add_argument("--config-dir", dest="config_dir", default=None,
                    help="Configuration directory. All config files with .yml extension will be processed one by one. "
                         "")
parser.add_argument("--config-encoding", dest="config_encoding", default="utf-8",
                    help="Encoding for the config file(s), defaults to utf-8")
parser.add_argument('-n', "--count", dest="count", default=1, type=int,
                    help="Number of test runs. Default is one. Use -1 to run indefinitely.")
parser.add_argument('-w', "--wait", dest="wait", default=10, type=float,
                    help="Number of seconds between test runs.")
parser.add_argument("-s", "--silent", dest='silent', action="store_true", default=False,
                    help="Supress all messages except errors.")
parser.add_argument("-v", "--verbose", dest='verbose', action="store_true", default=False,
                    help="Be verbose."
                    )
parser.add_argument("--halt-on-send-error", dest="halt_on_send_error", default=False, action="store_true",
                    help="Halt when cannot send data to influxdb. The default is to ignore the error.")

args = parser.parse_args()
if args.silent and args.verbose:
    parser.error("Cannot use --silent and --verbose at the same time.")
if args.config is None:
    args.config = "pigflux.yml"
if (args.config is not None) and (args.config_dir is not None):
    parser.error("You must give either --config or --config-dir (exactly one of them)")

if args.count == 0:
    parser.error("Test run count cannot be zero.")

if args.wait <= 0:
    parser.error("Wait time must be positive.")

if args.config is None and args.congig_dir is None:
    config_files = ["config.yml"]
elif args.config:
    config_files = [args.config]
else:
    config_files = []
    for file_name in sorted(os.listdir(args.config_dir)):
        ext = os.path.splitext(file_name)[1]
        if ext.lower() == ".yml":
            fpath = os.path.join(args.config_dir, file_name)
            config_files.append(fpath)

index = 0
while args.count < 0 or index < args.count:

    if args.count != 1:
        info("Pass #%d started" % (index + 1))

    started = time.time()
    for config_file in config_files:
        if not os.path.isfile(config_file):
            parser.error("Cannot open %s" % config_file)
        config = load_app_config(open(config_file, "r", encoding=args.config_encoding))
        main()
    elapsed = time.time() - started

    index += 1

    last_one = (args.count > 0) and (index == args.count)
    if not last_one:
        remaining = args.wait - elapsed
        if remaining > 0:
            if not args.silent:
                info("Pass #%d elapsed %.2f sec, waiting %.2f sec for next." % (index, elapsed, remaining))
            time.sleep(args.wait)
    else:
        info("Pass #%d elapsed %.2f sec" % (index, elapsed))

    info("")
