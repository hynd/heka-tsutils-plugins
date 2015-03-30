# Heka Plugins

A few plugins for [Heka](https://github.com/mozilla-services/heka).

Mostly based around metrics (in particular [OpenTSDB](https://github.com/OpenTSDB/opentsdb) and [StatsD](https://github.com/etsy/statsd) integration).
Tries to decouple some of Heka's Graphitisms, relies on a semi-formal, generic message structure (basically 'Metric' and 'Value' Heka message Fields), rather than pre-formatted Graphite payloads.

## Lua
Additional info and config examples can be found in each of the .lua files.

### lua_decoders/opentsdb_raw.lua
Parses an OpenTSDB (or TCollector) formatted message into Heka message Fields.
Should work with various inputs, such as existing TCollector collectors spawned from ProcessInputs, or FileInputs, UdpInputs etc.

Strips any (optional) leading "put "s, adds the timestamp to Heka's `Timestamp` field, the metric name and value to configurable fields (Fields[name] and Fields[data.value] by default) and any tags into separate dynamic Fields (with an optional prefix for the field name).

### lua_decoders/statsd.lua
Parses a StatsD message into Heka Fields.
Intended to work with Heka's vanilla UdpInput, aggregated by the statsd_aggregator.lua Filter (see below).

### lua_filters/statsd_aggregator.lua
Performs StatsD style aggregation on a stream of data (similar to the existing StatAccumInput).
Counters, Gauges, Histograms (with configurable percentiles) and Sets are supported.

The fields_parse function can be used to extract field data embedded in the bucket name.

Upon each timer_event, separate messages of aggregated data will be generated and injected back into the router (analagous to StatsD's "flush").
There may be a considerable number of aggregate messages flushed (a histogram type will be at least 5 per metric, plus 3 for each percentile). You may need to increase the global \[hekad\] `max_timer_inject` configuration.

### lua_filters/dedupe.lua
Emits a new stream of deduplicated messages.
If the values of the variant_fields remain the same for successive datapoints, the message is withheld.  If any of those values change, or then difference between the current and previously seen message Timestamp exceeds the "`dedupe_window`", both the previous and current message are emitted (to maintain graph slopes).

### lua_filters/fieldfix.lua
Performs some basic mutations on message Fields - add if not present, override, remove and rename.  New messages will be emitted with a new Type.

### lua_filters/heka_stats.lua
Converts Heka's all-report JSON and memstat messages into individual "metric" type events.  Not really intended for long-term use, just to get stats into OpenTSDB for testing.

### lua_encoders/opentsdb_raw.lua
Extracts data from message fields and generates JSON suitable for use with OpenTSDB's TCP input.

### lua_encoders/opentsdb_http.lua
Extracts Field data from messages and generates some OpenTSDB-compliant JSON.


## Go
The above Lua plugins are better maintained than these.
To include the Go plugins in a Heka build, per the [docs](https://hekad.readthedocs.org/en/latest/installing.html#building-hekad-with-external-plugins), create/add a line to a __{heka root}/cmake/plugin_loader.cmake__ file:
```
add_external_plugin(git https://github.com/hynd/heka-tsutils-plugins master __ignore_root statsd opentsdb)
```

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
* `Fields[Modifier]` - the short code representing "type" - **c**, **g**, **ms** or **s**
* `Fields[Sampling]` - the sample rate used by counters

See https://github.com/etsy/statsd/blob/master/docs/metric_types.md for more info.


## Things To Do
* Unit tests, benchmarking, docs...
* More validation of OpenTSDB input (allowed characters etc)
