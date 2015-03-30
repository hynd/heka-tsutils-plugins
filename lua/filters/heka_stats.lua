-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Convert Heka's all-report JSON and memstat messages into individual "metric"
type events.  Not really intended for long-term use, just to get stats into
OpenTSDB for testing.

Will run at the same interval that the Dashboard triggers a dump of
the report messages.
For the global state and each plugin, multiple new messages will be injected -
the 'max_process_inject' [hekad] config will need raising appropriately.

Config:
- metric_field (string, optional, default "Metric")
    Field name to use for the metric name

- value_field (string, optional, default "Value")
    Field name to use for the metric value

- tag_prefix (string, options, default "")
    Prefix to add to the "plugin" and "section" Field names

- global_prefix (string, optional, default "heka.report.")
    Prefix every metric name with a string.

*Example Heka Configuration*

.. code-block:: ini

[internal_stats]
type = "SandboxFilter"
filename = "lua_filters/heka_stats.lua"
message_matcher = "Type == 'heka.all-report' || Type == 'heka.memstat'"

[tsd_encoder]
type = "SandboxEncoder"
filename = "lua_encoders/opentsdb_raw.lua"
[tsd_encoder.config]
fields_to_tags = true

[tsd_output]
type = "TcpOutput"
address = "tsd:4242"
#type = "LogOutput"
message_matcher = "Type == 'heka.sandbox.internal_stats'"
encoder = "tsd_encoder"

--]]

require "cjson"
require "string"

local metric_field   = read_config("metric_field") or "Metric"
local value_field    = read_config("value_field") or "Value"
local tag_prefix     = read_config("tag_prefix") or ""
local global_prefix  = read_config("global_prefix") or "heka.report."

local sections       = {"globals", "decoders", "filters", "encoders"}
local plugins        = {}
local last_report    = 0

function process_message ()

    local t  = read_message("Type")
    local ts = read_message("Timestamp")

    local msg = { Type = "internal_stats",
                  Timestamp = ts,
                  Fields = {}
                }

    if t == "heka.memstat" then

      local fields = {"HeapSys","HeapAlloc","HeapIdle","HeapInuse","HeapReleased","HeapObjects"}
      for i,v in ipairs(fields) do
        local n = read_message(string.format("Fields[%s]", v))
        if type(n) == "number" then
          msg.Fields[metric_field]     = global_prefix .. string.lower(v)
          msg.Fields[value_field]      = n
          inject_message(msg)
        end
      end

    elseif t == "heka.all-report" then

      local ok, json = pcall(cjson.decode, read_message("Payload"))
      if not ok then return -1 end

      for n,section in ipairs(sections) do
          local t = json[section] or {}
          for i,v in ipairs(t) do
              if not v.Name then return -1 end
              for j, w in pairs(v) do
              if type(w) == "table" then
                  msg.Fields[metric_field]          = global_prefix .. string.lower(j)
                  msg.Fields[value_field]           = w.value
                  msg.Fields[tag_prefix.."plugin"]  = string.lower(v.Name)
                  msg.Fields[tag_prefix.."section"] = section
                  inject_message(msg)
              end
              end
          end
      end

    end

    return 0
end

function timer_event(ns) end
