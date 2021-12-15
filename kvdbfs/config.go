package kvdbfs

import (
	"encoding/json"
	"io/ioutil"
)

type MetaStoreConf struct {
	Dir         string `json:"dir"` // path to metastore
	SegmentSize int64  `json:"segment_size"`
	Prefix      string `json:"prefix"`
	FSRoot      string `json:"fs_root"`
	FSInfo      string `json:"fs_info"`
}

// Config implements the local configuration directives.
type Config struct {
	Name      string        `json:"name"`      // Identifier for replica lists
	LogLevel  string        `json:"log_level"` // Minimum level to log at (debug, info, warn, error, critical)
	ReadOnly  bool          `json:"readonly"`  // Whether or not the FS is read only
	Path      string        `json:"-"`         // Path the config was loaded from
	MetaStore MetaStoreConf `json:"meta_store"`
}

//===========================================================================
// Config Methods
//===========================================================================

// Load a configuration from a path on disk by deserializing the JSON data.
func (conf *Config) Load(path string) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	// Unmarshal the JSON data
	if err := json.Unmarshal(data, &conf); err != nil {
		return err
	}

	// Save the loaded path
	conf.Path = path
	return nil
}

// Dump a configuration to JSON to the path on disk. If dump is an empty
// string then will dump the config to the path it was loaded from.
func (conf *Config) Dump(path string) error {
	if path == "" {
		path = conf.Path
	}

	// Marshal the JSON configuration data
	data, err := json.Marshal(conf)
	if err != nil {
		return err
	}

	// Write the data to disk
	return ioutil.WriteFile(path, data, 0644)
}
