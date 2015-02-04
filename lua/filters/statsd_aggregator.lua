-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

-- Contributor(s):
--   Kieren Hynd <kieren@ticketmaster.com)

--[[
Performs StatsD style aggregation on a stream of data (similar to the existing
StatAccumInput).  The messages should be formed of a single StatsD packet split
into the following (configurable) fields:

  Fields[Metric]   (the StatsD 'bucket' name)
  Fields[Value]    (the numeric value of the metric)
  Fields[Modifier] (the short code representing 'type' - "c", "g", "ms" or "s")
  Fields[Sampling] (the sample rate used by counters)

Counters, Gauges, Histograms (with configurable percentiles) and Sets are
supported.  (See
https://github.com/etsy/statsd/blob/master/docs/metric_types.md for info)

The StatsdDecoder plugin can be used in conjunction with a regular UdpInput to
break StatsD packets into these fields.

Upon each timer_event, separate messages of aggregated data will be generated
and injected back into the router (analagous to StatsD's "flush").

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

- msg_type (string, optional, default "statsd.agg")
    Sets the message 'Type' header to the specified value.  It will be automatically
    and unavoidably prefixed with 'heka.sandbox.'.

- metric_field (string, optional, default "Metric")
    Name of Field used to store the bucket name.

- value_field (string, optional, default "Value")
    Name of Field used to store the value.

- modifier_field (string, optional, default "Modifier")
    Name of Field used to store the "modifier" (ie; "c", "ms" etc.)

- sampling_field (string, optional, default "Sampling")
    Name of Field used to store the sampling rate

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
local percentiles_str = read_config("percentiles") or "50,75,90,99"
local send_idle       = read_config("send_idle_stats") or false
local calc_rates      = read_config("calculate_rates") or false
local msg_type        = read_config("msg_type") or "statsd.agg"

local metric_field    = read_config("metric_field") or "Metric"
local value_field     = read_config("value_field") or "Value"
local modifier_field  = read_config("modifier_field") or "Modifier"
local sampling_field  = read_config("sampling_field") or "Sampling"

if global_prefix:len() >0 and global_prefix:sub(-1) ~= "." then 
  global_prefix = global_prefix .. "."
end

local percentiles = {}
if percentiles_str:len() >0 then
  for pct in percentiles_str:gmatch("[%d.]+") do
    percentiles[#percentiles+1] = pct
  end
end

buckets          = {}
lastTime         = os.time() * 1e9
metrics_received = 0

function process_message ()
    local metric   = read_message("Fields["..metric_field.."]")
    local value    = read_message("Fields["..value_field.."]")
    local modifier = read_message("Fields["..modifier_field.."]")
    local sampling = read_message("Fields["..sampling_field.."]") or 1

    if not metric or not value or not modifier then return -1 end

    if not buckets[metric] then
      -- create a new message template for the metric
      buckets[metric] = {
        Type        = msg_type,
        Fields      = {}
      }
      buckets[metric].Fields[sampling_field] = sampling
      buckets[metric].Fields[modifier_field] = modifier

      if modifier == "ms" then
        buckets[metric].Fields[value_field]  = {value}
        buckets[metric].Fields[metric_field] = global_prefix .. 'timers.' .. metric
      elseif modifier == "g" then
        buckets[metric].Fields[value_field]  = value
        buckets[metric].Fields[metric_field] = global_prefix .. 'gauges.' .. metric
      elseif modifier == "s" then
        buckets[metric].Fields[value_field]  = {}
        buckets[metric].Fields[value_field][value] = true
        buckets[metric].Fields[metric_field] = global_prefix .. 'sets.' .. metric
      else
        buckets[metric].Fields[value_field]  = value * (1/sampling)
        buckets[metric].Fields[metric_field] = global_prefix .. 'counters.' .. metric
      end

    else
      -- otherwise, just update the value
      if modifier == "ms" then
          buckets[metric].Fields[value_field][#buckets[metric].Fields[value_field]+1] = value
      elseif modifier == "g" then
          buckets[metric].Fields[value_field] = value
      elseif modifier == "s" then
          buckets[metric].Fields[value_field][value] = true
      else
          buckets[metric].Fields[value_field] = buckets[metric].Fields[value_field] + (value * (1/sampling))
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
      if msg.Fields[modifier_field] == "ms" and #msg.Fields[value_field] > 0 then

        local timers = msg.Fields[value_field]
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

        if stats.count > 1 and #percentiles > 0 then
          for _, pct in ipairs(percentiles) do
            local tmp = ((100 - pct) / 100) * stats.count
            local inc = stats.count - math.floor(tmp+0.5)

            if inc > 0 then
              stats["mean_"  .. pct] = cumVals[inc] / inc
              stats["sum_"   .. pct] = cumVals[inc]
              stats["upper_" .. pct] = timers[inc]
            end
          end

        end

        local metric_temp = msg.Fields[metric_field]
        for i, j in pairs(stats) do
          msg.Fields[metric_field] = metric_temp .. '.' .. i
          msg.Fields[value_field]  = j
          inject_message(msg)
        end

        if send_idle then
          msg.Fields[value_field]  = {}
          msg.Fields[metric_field] = metric_temp
        end

      -- counters
      elseif msg.Fields[modifier_field] == "c" then
        local metric_temp = msg.Fields[metric_field]
        msg.Fields[metric_field] = metric_temp .. '.count'
        inject_message(msg)

        if calc_rates then
          msg.Fields[metric_field] = metric_temp .. '.rate'
          msg.Fields[value_field]  = msg.Fields[value_field] / ( elapsedTime / 1e9 )
          inject_message(msg)
        end
        msg.Fields[metric_field] = metric_temp

        if send_idle then msg.Fields[value_field] = 0 end

      -- sets
      elseif msg.Fields[modifier_field] == "s" then
        local set_count = 0
        for k, _ in pairs(msg.Fields[value_field]) do set_count = set_count + 1 end
        msg.Fields[value_field] = set_count
        inject_message(msg)

        if send_idle then msg.Fields[value_field] = {} end

      -- gauges
      elseif msg.Fields[modifier_field] == "g" then
        inject_message(msg)

      end

      bucket_count = bucket_count + 1
    end

    local summary_msg = {
      Type        = msg_type,
      Fields      = {}
    }
    summary_msg.Fields[metric_field] = global_prefix .. 'numStats'
    summary_msg.Fields[value_field]  = bucket_count
    inject_message(summary_msg)

    summary_msg.Fields[metric_field] = global_prefix .. 'metrics_received'
    summary_msg.Fields[value_field]  = metrics_received
    inject_message(summary_msg)

    if not send_idle then buckets = {} end
end
