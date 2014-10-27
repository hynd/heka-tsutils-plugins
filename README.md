# Heka Plugins

A few random (work-in-progress) plugins for [Heka](https://github.com/mozilla-services/heka).

Mostly based around metrics (in particular [OpenTSDB](https://github.com/OpenTSDB/opentsdb) and [StatsD](https://github.com/etsy/statsd) integration).
Tries to decouple some of Heka's Graphitisms, relies on a semi-formal, generic message structure (basically 'Metric' and 'Value' Heka message Fields), rather than pre-formatted Graphite payloads.


## OpenTsdbRawDecoder
A Go-based OpenTSDB decoder.  Works with various inputs, such as existing [TCollector collectors](https://github.com/OpenTSDB/tcollector/tree/master/collectors/0) spawned from Process(Directory)Input's, or FileInputs, UdpInputs etc.

Strips any (optional) leading "put ", discards empty lines, performs a few basic sanity checks (a line isn't greater than 1KB in size and has at least 3 components).

Adds the timestamp to Heka's `Timestamp` field, the metric name to `Fields[Metric]` and value to `Fields[Value]` and splits any tags into separate dynamic Fields.  The Heka Message `Type` is set to "statsd".

* `tagname_prefix` (string, optional) - Prefix to add to any fields derived from tags, to make Field identification further down the pipeline easier


## OpenTsdbRawEncoder
A Go-based OpenTSDB encoder.  Works in conjunction with Heka's TcpOutput and messages following the format created by the OpenTsdbRawDecoder (ie; containing `Fields[Metric]` and `Fields[Value]`).
Supports OpenTSDB's "tags" which can be pulled from additional Heka Message fields, or delimited data embedded in the Metric name (making StatsD-generated metrics more flexible).

Supports a basic dedupe facility (emulating TCollector) where unchanging datapoints are discarded.  When the value for a metric/tag combination changes (or the `dedupe_window` is exceeded), both the last seen and current datapoints are sent to maintain graph slopes.

* `tagname_prefix` (string, optional) - If set, try to extract any embedded tag data from the metric named delimited by this value
* `tagvalue_prefix` (string, optional, default: `"."`) - Used to differentiate embedded tag names from values
* `ts_from_message` (bool, optional, default: `true`) - Set the timestamp based on the Message's `Timestamp` field or "Now()"
* `fields_to_tags` (bool, optional, default: `true`) - Convert any fields prefixed with `tagname_prefix` to OpenTSDB tags
* `dedupe_window` (uint, optional, default: `0` - off) - Activate dedupe, defines maximum window (in seconds)
* `tags_if_missing` (array, optional) - If set, an array of tags (`["tagk=tagv", "tagx=tagy"]`) to add to the output if not already present
* `tags_override` (array, optional) - If set, an array of tags to add to the output, overriding any set with the same tag name

## StatsdDecoder
A Go-based StatsD decoder.  Intended to work with Heka's vanilla UdpInput (rather than the dedicated StatsdInput/StatAccumInput).  Creates more generic field-based messages which can be aggregated, further filtered, and encoded for outputs other than Graphite.

The Heka Message `Type` is set to "statsd".  The message received from the Heka input should be formed of a single StatsD packet, split into the following fields:

* `Fields[Metric]`   - the StatsD "bucket" name
* `Fields[Value]`    - the numeric value of the metric
* `Fields[Modifier]` - the short code representing "type" - **c**, **g** or **ms**
* `Fields[Sampling]` - the sample rate used by counters

The "set" type is currently unsupported.
See https://github.com/etsy/statsd/blob/master/docs/metric_types.md for more info.


## statsd_aggregator.lua
A Lua-based StatsD aggregating SandboxFilter.  Performs StatsD style aggregation (similar to the existing StatAccumInput) on a stream of messages decoded by the above `StatsdDecoder`.  Supports Counters, Gauges and Histograms.  Adds configurable percentiles.

Upon each `ticker_interval`, separate messages of aggregated data will be generated and injected back into the router (analagous to StatsD's "flush").  The metric name will be in `Fields[Metric]` and value in `Fields[Value]`.

There may be a considerable number of aggregate messages flushed (a histogram type will be at least 5 per metric, plus 3 for each percentile). You may need to increase the global \[hekad\] `max_timer_inject` configuration.

* `ticker_interval` (uint) - delay between "flushes" of aggregate data (implies that some counters and histograms will be reset)
* `global_prefix` (string, optional) - Prefix every metric sent with a string
* `percentiles` (string, optional, default: `"50,75,90,99"`) - For histograms, a comma-separated list of percentiles to calculate
* `send_idle_stats` (bool, optional, default: `false`) - For "idle" metrics that haven't been received since the last flush (`ticker_interval`) we will send the previous seen value (for gauges) and a zero (for counters)
* `calculate_rates` (bool, optional, default: `false`) - Calculate "rate" (over `ticker_interval`) for counter and histogram metrics
* `type` (string, optional, default: `"statsd.agg"`) - Sets the message 'Type' header to the specified value.  It will be automatically and unavoidably prefixed with 'heka.sandbox.'

### Example Heka Configuration
```
    [StatsdUdpInput]
    type = "UdpInput"
    address = ":8125"
    parser_type = "regexp"
    delimiter = "(?:$|\n)"
    decoder = "StatsdDecoder"

    [StatsdDecoder]

    [StatsdFilter]
    type = "SandboxFilter"
    filename = "lua_filters/statsd_aggregator.lua"
    ticker_interval = 10
    message_matcher = "Type == 'statsd'"

    [StatsdFilter.config]
    quantiles = "50,95,99"
    global_prefix = "stats."
    send_idle_stats = true
```

## Things To Do
* Unit tests, benchmarking, docs...
* Handle the StatsD "set" type.
* More validation of OpenTSDB input (allowed characters, enforcing tags)
