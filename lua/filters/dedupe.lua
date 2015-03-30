-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

-- Contributor(s):
--   Kieren Hynd (kieren@ticketmaster.com)

--[[
Emits a new stream of deduplicated messages.

If the values of the variant_fields remain the same for successive
datapoints, the message is withheld.  If any of those values change,
or then difference between the current and previously seen message
Timestamp exceeds the 'dedupe_window', both the previous and current
message are emitted (to maintain graph slopes).

For example, a stream of messages, '.' represent seconds
(dedupe_window is set to 3):

tick:    .  .  .  .  .  .  .  .  .  .  .  .  .  .
input:   1  2  2  2  3  1  1  1              1  2
deduped: 1  2        23 1                    11 2

The global [hekad] "max_process_inject" option will need to be configured to at
least '2' in order for the process_message() function to send the previous and
current message.

Config:
- dedupe_window (uint, default 0)
    Number of seconds during which dedupe will be performed.  Timestamps
    deltas larger than this window will not be deduped.
    The default of 0 will perform no deduplication.

- variant_fields (string)
    Space separated list of Fields that will be compared to the previous
    message.  If all match, the message is eligible for dedupe.
    The remaining Fields in the message are considered 'invariants' and
    are used to form a key along with the 'Logger' for the dedupe.

- dedupe_key_field (string, optional)
    If we've already precomputed a key that can be used to dedupe on, and it's
    stored in a field, use that.

- msg_type (string, optional, default "dedupe")
    Sets the message 'Type' to the specified value (which will also have
    'heka.sandbox.' automatically and unavoidably prefixed)

- payload_keep (bool, default false)
    If true, maintain the original Payloads in the new messages.

- ticker_interval (uint)
    Frequency of self-stats reporting.
- metric_field (string, optional, default "Metric")
    Name of Field used to store the stat name.
- value_field (string, optional, default "Value")
    Name of Field used to store the stat value.

*Example Heka Configuration*

.. code-block:: ini

    [DedupeFilter]
    type = "SandboxFilter"
    filename = "lua_filters/dedupe.lua"
    [DedupeFilter.config]
    dedupe_window = 300
--]]

require "string"
require "table"

_PRESERVATION_VERSION     = read_config("preservation_version") or 0
local dedupe_window       = read_config("dedupe_window") or 0
local dedupe_key          = read_config("dedupe_key_field")
local variant_fields_str  = read_config("variant_fields") or ""
local msg_type            = read_config("msg_type") or "dedupe"
local payload_keep        = read_config("payload_keep")
local metric_field        = read_config("metric_field") or "Metric"
local value_field         = read_config("value_field") or "Value"

local variant_fields = {}
if variant_fields_str:len() > 0 then
  for field in variant_fields_str:gmatch("[%S]+") do
    variant_fields[field] = true
  end
end

buffer = {}
dedupe_count  = 0
buffer_size   = 0

function process_message()

  if dedupe_window > 0 and variant_fields_str then

    local timestamp   = read_message("Timestamp")
    local fields      = {}  -- all fields from the message
    local variants    = {}  -- fields that can change
    local invariants  = {}  -- fields that form the dedupe key

    -- populate the tables from the message Fields
    while true do
      local typ, name, value, representation, count = read_next_field()
      if not typ then break end
        if variant_fields[name] then
          variants[name] = value
        else
          invariants[name] = value
        end
        fields[name] = value
    end

    -- if we've been supplied a precomputed dedupe key, use it
    if dedupe_key and fields[dedupe_key] then
      key = fields[dedupe_key]
    -- otherwise, generate a string from the sorted invariants
    else
      local keys = {}
      for k,v in pairs(invariants) do
        table.insert(keys, string.format("%s=%s", k, v))
      end
      table.sort(keys)
      key = table.concat(keys, ":")
    end

    -- generate a new message
    local message = {
      Timestamp  = timestamp,
      Type       = msg_type,
      EnvVersion = read_message("EnvVersion"),
      Fields     = fields
    }
    if payload_keep then message.Payload = read_message("Payload") end

    -- if we've already stored a previous message
    if buffer[key] then

      -- check if the values of the previous variants match the current
      local match = true
      for k,v in pairs(variants) do
        if buffer[key].data[k] ~= v then
          match = false
          break
        end
      end

      -- if they match and we're inside the window, dedupe (store and return)
      if match and timestamp - buffer[key].timestamp < dedupe_window * 1e9 then
        buffer[key] = { message = message, data = variants, timestamp = buffer[key].timestamp, skipped = true }
        dedupe_count = dedupe_count + 1
        return 0
      end

      -- otherwise, if the value has changed and we've either skipped already or
      -- are outside the window, flush the previous message too
      if (not match and buffer[key].skipped) or
          (buffer[key].skipped and timestamp - buffer[key].timestamp >= dedupe_window * 1e9) then
        inject_message(buffer[key].message)
      end

    else
      buffer_size = buffer_size + 1
    end

    -- store as the new previous message
    buffer[key] = { message = message, data = variants, timestamp = timestamp, skipped = false }

    inject_message(message)
  end

  return 0
end

function timer_event(ns)
  local summary_msg = {
    Type        = msg_type,
    Fields      = {}
  }
  summary_msg.Fields[metric_field] = "heka.dedupe.saved"
  summary_msg.Fields[value_field]  = dedupe_count
  inject_message(summary_msg)

  summary_msg.Fields[metric_field] = "heka.dedupe.buffer_size"
  summary_msg.Fields[value_field]  = buffer_size
  inject_message(summary_msg)
end
