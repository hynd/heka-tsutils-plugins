-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

-- Contributor(s):
--   Kieren Hynd <kieren@ticketmaster.com)

--[[
Performs StatsD style aggregation on a stream of data.
The messages should be formed of a single StatsD packet split into the
following fields:

  Fields[Metric]   (the StatsD 'bucket' name)
  Fields[Value]    (the numeric value of the metric)
  Fields[Modifier] (the short code representing 'type' - "c", "g", "ms" or "s")
  Fields[Sampling] (the sample rate used by counters)

(See https://github.com/etsy/statsd/blob/master/docs/metric_types.md for info)

The Heka StatsdDecoder plugin can be used in conjunction with a regular UdpInput
to break StatsD packets into these fields.

Upon each timer_event, separate messages of aggregated data will be generated
and injected back into the router (analagous to StatsD's "flush").
The metric name will be in Fields[Metric] and value in Fields[Value].

There may be a considerable number of aggregate messages flushed (a histogram
type will be at least 5 per metric, plus 3 for each percentile).
You may need to increase the global 'max_timer_inject' [hekad] configuration.

Config:

- ticker_interval (uint)
    Delay between "flushes" of aggregate data (implies that some counters
    and histograms will be reset)

- global_prefix (string, optional)
    Prefix every metric sent with a string.

- percentiles (string, optional, default "50,75,90,99")
    For histograms, a comma-separated list of percentiles to calculate.

- send_idle_stats (bool, optional, default false)
    For "idle" metrics that haven't been received since the last flush (ticker_interval)
    we will send the previous seen value (for gauges) and a zero (for counters).

- calculate_rates (bool, optional, default false)
    Calculate "rate" (over ticker_interval) for counter and histogram metrics.

- type (string, optional, default "statsd.agg"):
    Sets the message 'Type' header to the specified value.  It will be automatically
    and unavoidably prefixed with 'heka.sandbox.'.

*Example Heka Configuration*

.. code-block:: ini

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

--]]

require "math"
require "string"
require "table"
require "os"

local global_prefix   = read_config("global_prefix") or ""
local percentiles     = read_config("percentiles") or "50,75,90,99"
local send_idle       = read_config("send_idle_stats") or false
local calc_rates      = read_config("calculate_rates") or false
local msg_type        = read_config("type") or "statsd.agg"

if global_prefix:len() >0 and global_prefix:sub(-1) ~= "." then 
  global_prefix = global_prefix .. "."
end

buckets          = {}
lastTime         = os.time() * 1e9
metrics_received = 0
--items       = {}

function process_message ()
    local metric   = read_message("Fields[Metric]")
    local value    = read_message("Fields[Value]")
    local sampling = read_message("Fields[Sampling]")
    local modifier = read_message("Fields[Modifier]")

    if not metric   or not value or 
       not sampling or not modifier then return -1 end

    if not buckets[metric] then
      -- create a new message template for the metric
      buckets[metric] = {
        Timestamp   = nil,
        Type        = msg_type,
        Payload     = nil,
        Fields      = {
          Sampling  = sampling,
          Modifier  = modifier
        }
      }
      if modifier == "ms" then
        buckets[metric].Fields.Value = {value}
        buckets[metric].Fields.Metric = global_prefix .. 'timers.' .. metric
      elseif modifier == "g" then
        buckets[metric].Fields.Value = value
        buckets[metric].Fields.Metric = global_prefix .. 'gauges.' .. metric
      elseif modifier == "s" then
        buckets[metric].Fields.Value = {}
        buckets[metric].Fields.Value[value] = true
        buckets[metric].Fields.Metric = global_prefix .. 'sets.' .. metric
      else
        buckets[metric].Fields.Value = value * (1/sampling)
        buckets[metric].Fields.Metric = global_prefix .. 'counters.' .. metric
      end

    else
      -- otherwise, just update the value
      if modifier == "ms" then
          buckets[metric].Fields.Value[#buckets[metric].Fields.Value+1] = value
      elseif modifier == "g" then
          buckets[metric].Fields.Value = value
      elseif modifier == "s" then
          buckets[metric].Fields.Value[value] = true
      else
          buckets[metric].Fields.Value = buckets[metric].Fields.Value + (value * (1/sampling))
      end
    end

    metrics_received = metrics_received + 1
    return 0
end

function timer_event(ns)

    local elapsedTime = ns - lastTime
    if elapsedTime == 0 then return end
    lastTime = ns

    local bucket_count = 0
    for _, msg in pairs(buckets) do

      -- histograms
      if msg.Fields.Modifier == "ms" and #msg.Fields.Value > 0 then

        local timers = msg.Fields.Value
        local stats = {}

        stats.count = #timers
        table.sort(timers)

        local cumVals   = {}
        local cumulator = 0
        for i, val in ipairs(timers) do
          cumulator  = val + cumulator
          cumVals[i] = cumulator
        end

        stats.lower = timers[1]
        stats.upper = timers[stats.count]
        stats.sum   = cumVals[#cumVals]
        stats.mean  = stats.sum / stats.count
        if calc_rates then
          stats.rate  = stats.count / ( elapsedTime / 1e9 )
        end

        if stats.count > 1 and percentiles:len() > 0 then
          for pct in percentiles:gmatch("[%d.]+") do
            local tmp = ((100 - pct) / 100) * stats.count
            local inc = stats.count - math.floor(tmp+0.5)

            if inc > 0 then
              stats["mean_"  .. pct] = cumVals[inc] / inc
              stats["sum_"   .. pct] = cumVals[inc]
              stats["upper_" .. pct] = timers[inc]
            end
          end

        end

        local metric_temp = msg.Fields.Metric
        for i, j in pairs(stats) do
          msg.Fields.Metric = metric_temp .. '.' .. i
          msg.Fields.Value  = j
          inject_message(msg)
        end

        if send_idle then
          msg.Fields.Value  = {}
          msg.Fields.Metric = metric_temp
        end

      -- counters
      elseif msg.Fields.Modifier == "c" then
        local metric_temp = msg.Fields.Metric
        msg.Fields.Metric = metric_temp .. '.count'
        inject_message(msg)

        if calc_rates then
          msg.Fields.Metric = metric_temp .. '.rate'
          msg.Fields.Value  = msg.Fields.Value / ( elapsedTime / 1e9 )
          inject_message(msg)
        end
        msg.Fields.Metric = metric_temp

        if send_idle then msg.Fields.Value = 0 end

      -- sets
      elseif msg.Fields.Modifier == "s" then
        local set_count = 0
        for k, _ in pairs(msg.Fields.Value) do set_count = set_count + 1 end
        msg.Fields.Value = set_count
        inject_message(msg)

        if send_idle then msg.Fields.Value = {} end

      -- gauges
      elseif msg.Fields.Modifier == "g" then
        inject_message(msg)

      end

      bucket_count = bucket_count + 1
    end

    local summary_msg = {
      Timestamp   = nil,
      Type        = msg_type,
      Payload     = nil,
      Fields      = {
        Metric = global_prefix .. 'numStats',
        Value  = bucket_count
      }
    }
    inject_message(summary_msg)
    summary_msg.Fields.Metric = global_prefix .. 'metrics_received'
    summary_msg.Fields.Value  = metrics_received
    inject_message(summary_msg)

    if not send_idle then buckets = {} end
end
