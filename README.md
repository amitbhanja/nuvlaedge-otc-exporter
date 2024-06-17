# nuvlaedge-otc-exporter
An OTC exporter which would convert data into different formats and send to different destinations 
like elastic search, nuvla api and other cloud endpoints like S3, GCS etc.

TODO : Nuvla APi and other cloud endpoints support will come later.

For elasticsearch, the exporter would create a timeseries resource (<index_prefix>-<application_name>) if not present.
Then all the metric datapoints are stored. We take the data attributes along with metrics data and nuvla.deployment.uuid.
So, the exporter cannot in the current version create timeseries resource dynamically based on the data sent by the
application. We should provide in the configuration the list of metrics along with the data attributes.


## Configuration options

- elasticsearch
  - enabled: Enable/Disable the exporter (default: false)
  - endpoint: Elastic search endpoint (default: http://localhost:9200)
  - insecure: Skip SSL verification (default: false)
  - ca_file: CA certificate file for verifying the server certificate (default: "")
  - index_prefix: Prefix for the index patterns and templates used for timeseries resource
                   (default: "nuvlaedge-opentelemetry-")
  - MetricsTobeExported: List of metrics to be exported to elastic search 
    Format : <metricName>,<metricType>,<isDimension>
       Dimension metrics along with the timestamp would define the unique document in elastic search
       Example : "cpu.usage,counter,false"
- nuvla
  - enabled: Enable/Disable the exporter (default: false)
  - endpoint: Nuvla endpoint (default: https://nuvla.io)
  - api_key: Nuvla API key
  - api_secret: Nuvla API secret
  - resource_id: Resource ID of the timeseries resource to which the data should be sent

- sending_queue: Queue Settings (enabled by ) (
  Follow the Queue Settings config in https://github.com/open-telemetry/opentelemetry-collector/blob/main/exporter/exporterhelper/README.md)
- retry_on_failure : Retry on failure (Follow the retry on failure configuration in
  https://github.com/open-telemetry/opentelemetry-collector/blob/main/exporter/exporterhelper/README.md)