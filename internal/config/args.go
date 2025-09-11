package config

type CLIArgs struct {
	Verbose     bool   `short:"v" long:"verbose" description:"Show verbose information"`
	Debug       bool   `short:"d" long:"debug" description:"Show debug information"`
	ShowVersion bool   `long:"version" description:"Show version information and exit"`
	CPUProfile  string `long:"cpu-profile" description:"Write CPU profile to file"`
	MemProfile  string `long:"mem-profile" description:"Write MEM profile to file"`
	NetProfile  uint16 `long:"net-profile-port" description:"Start profile http server on PORT"`
}

type PigfluxCLIArgs struct {
	CLIArgs
	ConfigFile string `short:"c" long:"config" description:"Path to config file"`
}
