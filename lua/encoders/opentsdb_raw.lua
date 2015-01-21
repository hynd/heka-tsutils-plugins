-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[=[
Extracts data from message fields and generates JSON suitable for use with
OpenTSDB's TCP input.

Config:
- metric_field (string, required, default "Metric")
    Field name the metric name is stored in

- value_field (string, required, default "Value")
    Field name the metric value is stored in

- fields_to_tags (boolean, optional, default false)
    Converts any dynamic Fields to OpenTSDB k=v tags, excluding those
    defined as metric_field or value_field

- tag_prefix (string, optional, default "")
    Only convert Fields to OpenTSDB k=v tags if they match the tag_prefix.
    (tag_prefix is stripped from the tag name)

- ts_from_message (boolean, optional, default false)
    Use the Timestamp field (otherwise "now")

- add_hostname_if_missing (boolean, optional, default false)
    If no 'host' tag has been seen, append one with the value of the
    Hostname field.  Deprecated in favour of using the 'fieldfix' filter.

*Example Heka Configuration*

.. code-block:: ini

    [OpentsdbEncoder]
    type = "SandboxEncoder"
    filename = "lua_encoders/opentsdb_raw.lua"

    [opentsdb]
    type = "TcpOutput"
    message_matcher = "Type == 'opentsdb'"
    address = "my.tsdb.server:4242"
    encoder = "OpentsdbEncoder"

*Example Output*

.. code-block:: bash

    put my.wonderful.metric 1412790960 124255 foo=bar fish=baz

--]=]

require "string"
require "os"

local metric_field    = read_config("metric_field") or error("metric_field must be specified")
local value_field     = read_config("value_field") or error("value_field must be specified")
local fields_to_tags  = read_config("fields_to_tags")
local tag_prefix      = read_config("tag_prefix")
local ts_from_message = read_config("ts_from_message")
local add_hostname    = read_config("add_hostname_if_missing")

local tag_prefix_length = 0
if tag_prefix then
  tag_prefix_length = #tag_prefix
end

function process_message()

    local metric = read_message("Fields["..metric_field.."]")
    local value  = read_message("Fields["..value_field.."]")

    if not metric or not value then return -1 end

    local ts
    if ts_from_message then
      ts = read_message("Timestamp") / 1e9
    else
      ts = os.time()
    end

    add_to_payload(string.format("%s %s %s", metric, ts, value))

    local seen_hosttag = false
    -- add tags from dynamic fields
    if fields_to_tags then

      while true do
        local typ, name, value, representation, count = read_next_field()
        if not typ then break end

        -- don't add the metric/value fields as tags
        if name ~= metric_field or name ~= value_field then

            if tag_prefix_length > 0 then
              -- only add fields that match the tagname_prefix
              if name:sub(1, tag_prefix_length) == tag_prefix then
                name = name:sub(tag_prefix_length+1)
                add_to_payload(string.format(" %s=%s", name, value))
              end
            else
              add_to_payload(string.format(" %s=%s", name, value))
            end

            if name == "host" then seen_hosttag = true end

        end
      end
    end

    if not seen_hosttag and add_hostname then
      add_to_payload(string.format(" host=%s", read_message("Hostname")))
    end

    inject_payload()
    return 0
end
