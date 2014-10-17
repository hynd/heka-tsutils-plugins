# Heka Plugins

A few random (work-in-progress) plugins for [Heka](https://github.com/mozilla-services/heka).

Mostly based around metrics (in particular [OpenTSDB](https://github.com/OpenTSDB/opentsdb) and [StatsD](https://github.com/etsy/statsd) integration).
Tries to decouple some of Heka's Graphitisms, relies on a semi-formal, generic message structure (basically 'Metric' and 'Value' Heka message Fields), rather than pre-formatted Graphite payloads.


* **A Go-based OpenTSDB encoder and decoder.**  Intended to work with Heka's TcpOutput, and various inputs (either existing [TCollector collectors](https://github.com/OpenTSDB/tcollector/tree/master/collectors/0) spawned from Process(Directory)Input's, FileInputs, UdpInputs etc).
Most notably, the encoder supports OpenTSDB's "tags" which can be pulled from additional Heka Message fields, or delimited data embedded in the Metric name (making StatsD-generated metrics more flexible).
* **A Go-based StatsD decoder**.  Intended to work with Heka's vanilla UdpInput (rather than the dedicated StatsdInput/StatAccumInput).  Creates more generic field-based messages which can be aggregated, further filtered, and encoded for outputs other than Graphite.
* **A Lua-based StatsD aggregating Filter plugin.**  Emulates the functionality of the existing Go-based StatAccumInput.  Supports Counters, Gauges and Histograms.  Adds configurable percentiles.


## Things To Do
* Unit tests, benchmarking, docs...
* A dedupe filter
* Handle the StatsD "set" type.
