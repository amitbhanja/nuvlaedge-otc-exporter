package nuvlaedge_otc_exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"io"
	"os"
	"strings"
	"time"
)

var (
	typeStr = component.MustNewType("nuvlaedge_otc_exporter")
)

type NuvlaEdgeOTCExporter struct {
	cfg      *Config
	esClient *elasticsearch.Client
	settings component.TelemetrySettings
}

func newNuvlaEdgeOTCExporter(
	cfg *Config,
	set *exporter.CreateSettings,
) (*NuvlaEdgeOTCExporter, error) {
	return &NuvlaEdgeOTCExporter{
		cfg:      cfg,
		esClient: nil,
		settings: set.TelemetrySettings,
	}, nil
}

func convertToESConfig(cfg *ElasticSearchConfig, logger *zap.Logger) (elasticsearch.Config, error) {
	esConfig := elasticsearch.Config{
		Addresses: []string{cfg.Endpoint},
	}
	if !cfg.Insecure {
		cert, err := os.ReadFile(cfg.CaFile)
		if err == nil {
			logger.Error("Error reading CA file", zap.Error(err))
		}
		esConfig.CACert = cert
	}
	return esConfig, nil
}

func (e *NuvlaEdgeOTCExporter) Start(_ context.Context, _ component.Host) error {
	var err error
	var esConfig elasticsearch.Config
	esConfig, err = convertToESConfig(e.cfg.ElasticSearch_config, e.settings.Logger)
	e.esClient, err = elasticsearch.NewClient(esConfig)
	if err != nil {
		e.settings.Logger.Error("Error creating ElasticSearch client: ", zap.Error(err))
		return err
	}
	err = e.checkIndexTemplatesInElasticSearch()
	if err != nil {
		return err
	}
	return nil
}

func (e *NuvlaEdgeOTCExporter) checkIndexTemplatesInElasticSearch() error {
	req := esapi.IndicesGetIndexTemplateRequest{
		Name: e.cfg.ElasticSearch_config.IndexPrefix + "-*",
	}
	res, err := req.Do(context.Background(), e.esClient)
	if err != nil {
		e.settings.Logger.Error("Error checking index templates in ElasticSearch: ", zap.Error(err))
		return err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			e.settings.Logger.Error("Error closing the response body: ", zap.Error(err))
		}
	}(res.Body)

	if res.IsError() {
		e.settings.Logger.Error("Error checking index templates in ElasticSearch: ", zap.Error(err))
		return err
	}
	var templates map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&templates); err != nil {
		e.settings.Logger.Error("Error parsing the response body: ", zap.Error(err))
	}

	e.settings.Logger.Info("Index templates in ElasticSearch: ", zap.Any("templates", templates))
	indexTemplates, ok := templates["index_templates"].([]interface{})
	if !ok {
		e.settings.Logger.Error("Error parsing the response body: ", zap.Error(err))
		return err
	}
	for _, templateIter := range indexTemplates {
		template, templateOk := templateIter.(map[string]interface{})
		if !templateOk {
			e.settings.Logger.Error("TemplateIter is not map[string]interface{}:", zap.Any("templateIter", templateIter))
			continue
		}
		templateName := template["name"].(string)
		templatesPresent[templateName] = true
	}

	return nil
}

func (e *NuvlaEdgeOTCExporter) createTSDSTemplate(indexPattern *string) map[string]interface{} {
	template := map[string]interface{}{
		"index_patterns": []string{*indexPattern},
		"data_stream":    map[string]interface{}{},
		"template": map[string]interface{}{
			"settings": map[string]interface{}{
				"index.mode": "time_series",
			},
			"mappings": map[string]interface{}{
				"properties": map[string]interface{}{
					"@timestamp": map[string]interface{}{
						"type": "date",
					},
				},
			},
		},
	}

	for _, metricExport := range e.cfg.ElasticSearch_config.MetricsTobeExported {
		keys := strings.Split(metricExport, ",")
		if len(keys) != 3 {
			e.settings.Logger.Error("Require three parameters <metric_name>,<metric_type>,<is_dimension>"+
				" ", zap.String("metric", metricExport))
			continue
		}
		metricName := keys[0]
		metrictype := keys[1]
		isDimension := keys[2]

		if isDimension == "true" {
			template["template"].(map[string]interface{})["mappings"].(map[string]interface{})["properties"].(map[string]interface{})[metricName] = map[string]interface{}{
				"type":                  "keyword",
				"time_series_dimension": "true",
			}
		} else {
			valueType := "long"
			if metrictype == "gauge" {
				valueType = "double"
			}
			template["template"].(map[string]interface{})["mappings"].(map[string]interface{})["properties"].(map[string]interface{})[metricName] = map[string]interface{}{
				"type":               valueType,
				"time_series_metric": metrictype,
			}
		}
	}
	return template
}

func (e *NuvlaEdgeOTCExporter) createNewTSDS(timeSeries string) error {
	templateName := fmt.Sprintf("%s-%s-template", e.cfg.ElasticSearch_config.IndexPrefix, timeSeries)
	if _, ok := templatesPresent[templateName]; !ok {
		indexPattern := fmt.Sprintf("%s-%s*", e.cfg.ElasticSearch_config.IndexPrefix, timeSeries)
		template := e.createTSDSTemplate(&indexPattern)

		templateJSON, err := json.Marshal(template)
		if err != nil {
			e.settings.Logger.Error("Error marshaling the template: ", zap.Error(err))
			return err
		}

		e.settings.Logger.Info("Creating index template new print ", zap.String("templateName", templateName))
		// Create the index template
		req := esapi.IndicesPutIndexTemplateRequest{
			Name: templateName,
			Body: bytes.NewReader(templateJSON),
		}

		res, err := req.Do(context.Background(), e.esClient)
		if err != nil {
			e.settings.Logger.Error("Error creating the index template: ", zap.Error(err))
			return fmt.Errorf("error creating the index template: %s", err.Error())
		}
		defer func(Body io.ReadCloser) {
			errClose := Body.Close()
			if errClose != nil {
				e.settings.Logger.Error("Error closing the response body: ", zap.Error(errClose))
			}
		}(res.Body)

		responseStr, errorOccured := e.readElasticSearchResponse(res)
		if errorOccured {
			return fmt.Errorf("error creating the index template: %s", responseStr)
		}
		templatesPresent[templateName] = true
	}
	e.settings.Logger.Info("Index template created ", zap.String("timeSeries", timeSeries))
	return nil
}

func (e *NuvlaEdgeOTCExporter) readElasticSearchResponse(res *esapi.Response) (string, bool) {
	bodyBytes, errRes := io.ReadAll(res.Body)
	if errRes != nil {
		e.settings.Logger.Error("Error reading the response body: ", zap.Error(errRes))
		return errRes.Error(), false
	}
	bodyString := string(bodyBytes)
	if res.IsError() {
		e.settings.Logger.Error("Error performing operation in ES ", zap.String("bodyString", bodyString))
		return bodyString, false
	}
	e.settings.Logger.Info("Response from ES ", zap.String("bodyString", bodyString))
	return bodyString, true
}

func (e *NuvlaEdgeOTCExporter) ConsumeMetrics(_ context.Context, pm pmetric.Metrics) error {
	rms := pm.ResourceMetrics()

	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		attrs := rm.Resource().Attributes()
		nuvlaDeploymentUUID, _ := attrs.Get("nuvla.deployment.uuid")
		serviceVal, _ := attrs.Get("service.name")
		serviceName := serviceVal.Str()

		scm := rm.ScopeMetrics()
		for j := 0; j < scm.Len(); j++ {
			sc := scm.At(j)

			ms := sc.Metrics()
			if ms.Len() == 0 {
				continue
			}
			err := e.createNewTSDS(serviceName)
			e.settings.Logger.Info("Creating TSDS ", zap.String("timeSeriesName", serviceName), zap.Error(err))
			if err != nil {
				e.settings.Logger.Error("Error creating the TSDS: ", zap.Error(err))
				return err
			}
			uuid := nuvlaDeploymentUUID.Str()
			var metricMap []map[string]interface{}
			for k := 0; k < ms.Len(); k++ {
				currMetric := ms.At(k)
				updateMetric(&serviceName, &currMetric, &metricMap, &uuid)
			}
			indexName := fmt.Sprintf("%s-%s", e.cfg.ElasticSearch_config.IndexPrefix, serviceName)
			e.settings.Logger.Info("Adding documents in TSDS ", zap.String("indexName", indexName), zap.Any("metricMap", metricMap))
			err = e.addDocsInTSDS(&indexName, &metricMap)
			if err != nil {
				e.settings.Logger.Error("Error adding documents in TSDS: ", zap.Error(err))
				return err
			}
		}
	}
	return nil
}

func (e *NuvlaEdgeOTCExporter) addDocsInTSDS(timeSeries *string,
	metricMapDetails *[]map[string]interface{}) error {
	var buf bytes.Buffer

	meta := fmt.Sprintf("{ \"create\" : { } }\n")

	completeMetric := ""
	for _, currMetric := range *metricMapDetails {
		curr := meta
		curr = curr + "{ "
		for key, value := range currMetric {
			curr = curr + fmt.Sprintf("\"%s\": \"%v\", ", key, value)
		}
		curr = curr[:len(curr)-2]
		curr = curr + " }\n"
		completeMetric = completeMetric + curr
	}
	e.settings.Logger.Info("Complete Metric ", zap.String("completeMetric", completeMetric))
	byteComplete := []byte(completeMetric)
	buf.Grow(len(byteComplete))
	buf.Write(byteComplete)

	req := esapi.BulkRequest{
		Index: *timeSeries,
		Body:  &buf,
	}
	res, err := req.Do(context.Background(), e.esClient)
	if err != nil {
		e.settings.Logger.Error("Error adding documents in TSDS: ", zap.Error(err))
		return err
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			e.settings.Logger.Error("Error closing the response body: ", zap.Error(err))
		}
	}(res.Body)

	responseStr, errorOccured := e.readElasticSearchResponse(res)
	if errorOccured {
		return fmt.Errorf("error adding documents in TSDS: %s", responseStr)
	}
	return nil
}

func updateMetric(serviceName *string, metric *pmetric.Metric,
	metricMap *[]map[string]interface{}, deploymentuuid *string) {

	var dp pmetric.NumberDataPointSlice

	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dp = metric.Gauge().DataPoints()
	case pmetric.MetricTypeSum:
		dp = metric.Sum().DataPoints()
	default:
		panic("unhandled default case")
	}

	metricName := metric.Name()
	metricName, _ = strings.CutPrefix(metricName, *serviceName+"_")
	var currMetricMap = make(map[string]interface{})
	for i := 0; i < dp.Len(); i++ {
		datapoint := dp.At(i)
		// TODO there could be situations of timestamps being very close or same.
		// Need to handle that.
		//timestamp := datapoint.Timestamp().AsTime().Format("2006-01-02T15:04:05.999Z07:00")
		currMetricMap["@timestamp"] = time.Now().Format("2006-01-02T15:04:05.999Z07:00")
		time.Sleep(2 * time.Millisecond)
		currMetricMap["nuvla.deployment.uuid"] = *deploymentuuid

		switch datapoint.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			currMetricMap[metricName] = datapoint.IntValue()
		case pmetric.NumberDataPointValueTypeDouble:
			currMetricMap[metricName] = datapoint.DoubleValue()
		default:
			panic("unhandled default case")
		}

		datapoint.Attributes().Range(func(k string, v pcommon.Value) bool {
			currMetricMap[k] = v.AsString()
			return true
		})
		*metricMap = append(*metricMap, currMetricMap)
	}
}

var templatesPresent = make(map[string]bool)
