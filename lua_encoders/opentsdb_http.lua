--[[
Extracts Field data from messages and generates some OpenTSDB-compliant JSON
(http://opentsdb.net/docs/build/html/api_http/put.html).

Can optionally buffer and flush events after a configurable number of messages
(flush_count) or after a configurable number of seconds have passed between the
last Timestamp and the current one (flush_delta) (encoders currently have no
support for timer_event()s)

Config:

- flush_count (number, default 1)
    Flush the buffer every 'flush_count' messages.

- flush_delta (number, default 0)
    Flush the buffer if 'flush_delta' seconds have elapsed since the last
    Timestamp and the current one.

- metric_field (string, required, default "Metric")
    Field name the metric name is stored in

- value_field (string, required, default "Value")
    Field name the metric value is stored in

- fields_to_tags (boolean, optional, default false)
    Converts any dynamic Fields to OpenTSDB tags, excluding those
    defined as metric_field or value_field

- tag_prefix (string, optional, default "")
    Only convert Fields to OpenTSDB tags if they match the tag_prefix.
    (tag_prefix is stripped from the tag name)

- ts_from_message (boolean, optional, default false)
    Use the Timestamp field (otherwise "now")

- add_hostname_if_missing (boolean, optional, default false)
    If no 'host' tag has been seen, append one with the value of the
    Hostname field.  Deprecated in favour of using the 'fieldfix' filter.

--]]

require "cjson"
require "table"
require "string"

local metric_field    = read_config("metric_field") or "Metric"
local value_field     = read_config("value_field") or "Value"
local flush_count     = read_config("flush_count") or 1
local flush_delta     = read_config("flush_delta") or 0
local fields_to_tags  = read_config("fields_to_tags")
local tag_prefix      = read_config("tag_prefix")
local ts_from_message = read_config("ts_from_message")
local add_hostname    = read_config("add_hostname_if_missing")
_PRESERVATION_VERSION = read_config("preservation_version") or 0

local tag_prefix_len  = tag_prefix:len()

buffer = {}
last_flush = 0

function flush(ts)
  add_to_payload(cjson.encode(buffer))
  inject_payload()
  last_flush = ts
  buffer = {}
end

function process_message()

  local metric = read_message("Fields["..metric_field.."]")
  local value  = read_message("Fields["..value_field.."]")

  local ts
  if ts_from_message then
    ts = read_message("Timestamp") / 1e9
  else
    ts = os.time()
  end

  if not metric or not value or not ts then return -1 end

  local msg = { timestamp = ts,
                metric = metric,
                value = value,
                tags = {}
              }

  local seen_hosttag = false

  -- add tags from dynamic fields
  if fields_to_tags then

    while true do
      local typ, name, value, representation, count = read_next_field()
      if not typ then break end

      -- don't add the metric/value fields as tags
      if name ~= metric_field and name ~= value_field then

          if tag_prefix_len > 0 then
            -- only add fields that match the tagname_prefix
            if name:sub(1, tag_prefix_len) == tag_prefix then
              name = name:sub(tag_prefix_len+1)
              msg.tags[name] = value
            end
          else
            msg.tags[name] = value
          end

          if name == "host" then seen_hosttag = true end

      end
    end
  end

  if not seen_hosttag and add_hostname then
    msg.tags["host"] = read_message("Hostame")
  end

  -- add message to buffer
  buffer[#buffer+1] = msg

  -- flush the buffer
  if flush_count > 0 and #buffer >= flush_count then
    flush(ts)
  elseif flush_delta > 0 and ts - last_flush > flush_delta then
    flush(ts)
  else
    return -2
  end

  return 0

end
