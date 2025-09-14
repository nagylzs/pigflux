package pigflux

import (
	"database/sql"
	"log/slog"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/influxdata/influxdb-client-go/api"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/nagylzs/pigflux/internal/config"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type IConnV1 struct {
	Cfg    config.Influx
	Name   string
	Client client.HTTPClient
}

func ConnectInfluxes(cf config.Config, names []string) []IConnV1 {
	clientsv1 := make([]IConnV1, 0)
	for _, name := range names {
		icfg := cf.Influxes[name]
		conn, err := client.NewHTTPClient(client.HTTPConfig{
			Addr:     icfg.URL,
			Username: icfg.Username,
			Password: icfg.Password,
		})
		if err != nil {
			slog.Error("could not create influx v1 connection", "error", err)
		}
		clientsv1 = append(clientsv1, IConnV1{
			Cfg:    icfg,
			Name:   name,
			Client: conn,
		})
	}
	return clientsv1
}

func CloseInfluxes(clientsv1 []IConnV1) {
	for _, cl := range clientsv1 {
		err := cl.Client.Close()
		if err != nil {
			slog.Error("could not close influx v1 connection", "name", cl.Name, "error", err)
		}
	}
}

type I2ConnV2 struct {
	Cfg      config.Influx2
	Name     string
	Client   influxdb2.Client
	WriteAPI api.WriteAPIBlocking
}

func ConnectInfluxes2(cf config.Config, names []string) []I2ConnV2 {
	clientsv2 := make([]I2ConnV2, 0)
	for _, name := range names {
		icfg := cf.Influxes2[name]
		// Create a new client using an InfluxDB server base URL and an authentication token
		cl := influxdb2.NewClient(icfg.Url, icfg.Token)
		// Use blocking write client for writes to desired bucket
		writeAPI := cl.WriteAPIBlocking(icfg.Org, icfg.Bucket)
		clientsv2 = append(clientsv2, I2ConnV2{Cfg: icfg, Name: name, Client: cl, WriteAPI: writeAPI})
	}
	return clientsv2
}

func CloseInfluxes2(clientsv2 []I2ConnV2) {
	for _, cl := range clientsv2 {
		cl.Client.Close()
	}
}

type I2ConnV3 struct {
	Cfg  config.Influx3
	Name string
	Conn *influxdb3.Client
}

func ConnectInfluxes3(cf config.Config, names []string) []I2ConnV3 {
	clientsv3 := make([]I2ConnV3, 0)
	for _, name := range names {
		icfg := cf.Influxes3[name]
		cl, err := influxdb3.NewFromConnectionString(icfg.Url)
		if err != nil {
			slog.Error("could not create influx v3 connection", "name", name, "error", err)
		}
		clientsv3 = append(clientsv3, I2ConnV3{Cfg: icfg, Name: name, Conn: cl})
	}
	return clientsv3
}

func CloseInfluxes3(clientsv3 []I2ConnV3) {
	for _, cl := range clientsv3 {
		err := cl.Conn.Close()
		if err != nil {
			slog.Error("could not close influx v3 connection", "name", cl.Name, "error", err)
		}
	}
}

type I2ConnDb struct {
	Cfg  config.Database
	Name string
	Conn *sql.DB
}

func ConnectDatabases(cf config.Config, names []string) []I2ConnDb {
	dbconns := make([]I2ConnDb, 0)
	for _, name := range names {
		dcfg := cf.Databases[name]
		conn, err := sql.Open(dcfg.Driver, dcfg.DSN)
		if err != nil {
			slog.Error("unable to connect to database", "database", name, "error", err)
		}
		dbconns = append(dbconns, I2ConnDb{Cfg: dcfg, Name: name, Conn: conn})
	}
	return dbconns
}

func CloseDatabases(dbconns []I2ConnDb) {
	for _, cl := range dbconns {
		err := cl.Conn.Close()
		if err != nil {
			slog.Error("could not close database connection", "name", cl.Name, "error", err)
		}
	}
}
