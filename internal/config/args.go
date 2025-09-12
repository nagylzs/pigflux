package config

type CLIArgs struct {
	Silent      bool   `short:"s" long:"silent" description:"Suppress all messages except errors"`
	Verbose     bool   `short:"v" long:"verbose" description:"Be verbose (info)"`
	Debug       bool   `short:"d" long:"debug" description:"Show debug information"`
	ShowVersion bool   `long:"version" description:"Show version information and exit"`
	CPUProfile  string `long:"cpu-profile" description:"Write CPU profile to file"`
	MemProfile  string `long:"mem-profile" description:"Write MEM profile to file"`
	NetProfile  uint16 `long:"net-profile-port" description:"Start profile http server on PORT"`
}

type PigfluxCLIArgs struct {
	CLIArgs
	ConfigFiles []string `short:"c" long:"config" description:"Path to config file"`
	ConfigDirs  []string `long:"config-dir" description:"Path to config dir, all yml files will be loaded and executed."`
	Count       int      `long:"count" description:"Number of test runs. Defaults to 1. Use -1 to run indefinitely." default:"1"`
	Wait        string   `short:"w" long:"wait" description:"Time to wait between test runs. Defaults to 10s" default:"10s"`
}
