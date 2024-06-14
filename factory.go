package nuvlaedge_otc_exporter

import (
	"context"
	"errors"
	"fmt"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"net/url"
)

func createDefaultConfig() component.Config {
	return &Config{
		ElasticsearchConfig: &ElasticSearchConfig{
			Enabled:     false,
			Endpoint:    "http://localhost:9200",
			Insecure:    true,
			CaFile:      "",
			IndexPrefix: "nuvlaedge-opentelemetry-",
		},
		NuvlaApiConfig: &NuvlaApiConfig{
			Enabled: false,
		},
		QueueConfig: exporterhelper.NewDefaultQueueSettings(),
		RetryConfig: configretry.NewDefaultBackOffConfig(),
	}
}

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		typeStr,
		createDefaultConfig,
		exporter.WithMetrics(createMetricsExporter, component.StabilityLevelBeta),
	)
}

func createMetricsExporter(
	ctx context.Context,
	set exporter.CreateSettings,
	cfg component.Config,
) (exporter.Metrics, error) {
	fmt.Printf("createMetricsExporter being called \n")
	oCfg := cfg.(*Config)
	_, err := url.Parse(oCfg.ElasticsearchConfig.Endpoint)
	if err != nil {
		return nil, errors.New("endpoint must be a valid URL")
	}

	exp, err := newNuvlaEdgeOTCExporter(oCfg, &set)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewMetricsExporter(
		ctx, set, cfg,
		exp.ConsumeMetrics,
		exporterhelper.WithStart(exp.Start),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		// explicitly disable since we rely on http.Client timeout logic.
		exporterhelper.WithTimeout(exporterhelper.TimeoutSettings{Timeout: 0}),
		exporterhelper.WithRetry(oCfg.RetryConfig),
		exporterhelper.WithQueue(oCfg.QueueConfig),
	)
}
