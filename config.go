package nuvlaedge_otc_exporter

import (
	"errors"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type Config struct {
	ElasticSearch_config *ElasticSearchConfig `mapstructure:"elasticsearch"`
}

type ElasticSearchConfig struct {
	endpoint            string                       `mapstructure:"endpoint"`
	insecure            bool                         `mapstructure:"insecure"`
	caFile              string                       `mapstructure:"ca_file"`
	indexPrefix         string                       `mapstructure:"index_prefix"`
	QueueConfig         exporterhelper.QueueSettings `mapstructure:"sending_queue"`
	RetryConfig         configretry.BackOffConfig    `mapstructure:"retry_on_failure"`
	metricsTobeExported []string                     `mapstructure:"metrics"`
}

func (cfg *Config) Validate() error {
	if cfg.ElasticSearch_config.endpoint == "" {
		return errors.New("endpoint must be specified")
	}
	if !cfg.ElasticSearch_config.insecure && cfg.ElasticSearch_config.caFile == "" {
		return errors.New("need to give the ca_file if we want to secure connection")
	}
	if err := cfg.ElasticSearch_config.QueueConfig.Validate(); err != nil {
		return err
	}
	if err := cfg.ElasticSearch_config.RetryConfig.Validate(); err != nil {
		return err
	}
	return nil
}

func (cfg *Config) Unmarshal(conf *confmap.Conf) error {
	return conf.Unmarshal(cfg)
}

var _ component.Config = (*Config)(nil)
var _ confmap.Unmarshaler = (*Config)(nil)
