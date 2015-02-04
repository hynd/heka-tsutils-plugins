-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Parses an OpenTSDB (or TCollector) formatted message into Heka message Fields.
Should work with various inputs, such as existing TCollector collectors spawned
from ProcessInputs, or FileInputs, UdpInputs etc.

Strips any (optional) leading "put "s, adds the timestamp to Heka's `Timestamp`
field, the metric name and value to configurable fields (Fields[name] and
Fields[data.value] by default) and any tags into separate dynamic Fields (with
an optional prefix for the field name).

Config:
- payload_keep (bool, default false)
    If true, maintain the original Payload in the new message.

- msg_type (string, optional default "opentsdb")
    Sets the message 'Type' header to the specified value.

- name_field (string, optional, default "name")
    Field name to use for the metric name

- value_field (string, optional, default "data.value")
    Field name to use for the metric value

- tagname_prefix (string, options, default "tags.")
    Prefix to add to any Fields derived from tags, to make Field idenitication
    further down the pipeline easier.

*Example Heka Configuration*

.. code-block:: ini

    [OpenTsdbDecoder]
    type = "SandboxDecoder"
    filename = "lua_decoders/opentsdb_raw.lua"
    [OpenTsdbDecoder.config]
    payload_keep = true

*Example Heka Message*

2014/12/03 19:07:52
:Timestamp: Thu Dec 18 12:24:11 +0000 UTC
:Type: metric
:Hostname: test.example.com
:Pid: 25348
:Uuid: 190633d6-b11a-424c-ae73-3bcfb67c31fb
:Logger: ProcessInput
:Payload: put my.wonderful.metric 1418905451 42 product=wibble
:EnvVersion:
:Severity: 7
:Fields:
    | name:"name" type:string value:"my.wonderful.metric"
    | name:"tags.product" type:string value:"wibble"
    | name:"data.value" type:double value:42
--]]


local l = require 'lpeg'
local dt = require 'date_time'

local tagname_prefix = read_config("tagname_prefix") or "tags."
local name_field     = read_config("name_field") or "name"
local value_field    = read_config("value_field") or "data.value"
local msg_type       = read_config("msg_type") or "opentsdb"
local payload_keep   = read_config("payload_keep")

local function tagprefix(tag)
  if tagname_prefix then
    return tagname_prefix .. tag
  end
  return tag
end

l.locale(l)
local space     = l.space^1
local name      = (l.alnum + l.S"-._/") ^1
local integer   = l.S("-")^-1 * l.digit^1
local double    = integer * ("." * integer)^0
local number    = double * (l.S("Ee") * integer)^0 / tonumber
local timestamp = double / dt.seconds_to_ns

local metric    = l.P("put ")^0 * l.Cg(name, "metric")
local ts        = space * l.Cg(timestamp, "timestamp")
local value     = space * l.Cg(number, "value")
local pair      = space * l.Cg(name / tagprefix * "=" * l.C(name))
local tagset    = l.Cg(l.Cf(l.Ct("") * pair^0, rawset), "tags")

local grammar   = l.Ct(metric * ts * value * tagset)

function process_message ()

  local msg = {
    Type   = msg_type,
    Fields = {}
  }

  local line = read_message("Payload")
  local fields = grammar:match(line)
  if not fields then return -1 end

  msg.Timestamp           = fields.timestamp
  msg.Fields              = fields.tags
  msg.Fields[name_field]  = fields.metric
  msg.Fields[value_field] = fields.value

  if payload_keep then msg.Payload = line end

  inject_message(msg)
  return 0
end
