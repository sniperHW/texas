package main

import (
	"github.com/BurntSushi/toml"
)

func LoadConfig(path string) (*Config, error) {
	config := &Config{}
	_, err := toml.DecodeFile(path, config)
	if nil != err {
		return nil, err
	} else {
		return config, nil
	}
}

type Config struct {
	TaskCfg            string `toml:"TaskCfg"`
	DB                 string `toml:"DB"`
	WorkerService      string `toml:"WorkerService"`
	PauseInterval      int    `toml:"PauseInterval"`
	PauseTime          int    `toml:"PauseTime"`
	PauseBroadcastTime int    `toml:"PauseBroadcastTime"`
	ThreadReserved     int    `toml:"ThreadReserved"`

	Log struct {
		MaxLogfileSize int    `toml:"MaxLogfileSize"`
		LogDir         string `toml:"LogDir"`
		LogPrefix      string `toml:"LogPrefix"`
		LogLevel       string `toml:"LogLevel"`
		EnableStdout   bool   `toml:"EnableStdout"`
		MaxAge         int    `toml:"MaxAge"`
		MaxBackups     int    `toml:"MaxBackups"`
	} `toml:"Log"`
}
