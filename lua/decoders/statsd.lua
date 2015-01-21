-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Parses a StatsD message into Heka Fields.
Intended to work with Heka's vanilla UdpInput, aggregated by a Filter.

Config:
- payload_keep (bool, default false)
    If true, maintain the original Payload in the new message.

- msg_type (string, optional default "statsd")
    Sets the message 'Type' header to the specified value.

- metric_field (string, optional, default "Metric")
    Field name to use for the bucket name

- value_field (string, optional, default "Value")
    Field name to use for the stat value

- modifier_field (string, optional, default "Modifier")
    Field name to use for the stat's "modifier" (ie; "c", "ms" etc)

- sampling_field (string, optional, default "Sampling")
    Field name to use for the stat's sampling rate


*Example Heka Configuration*

.. code-block:: ini

    [UdpInput]
    address = ":8125"
    parser_type = "regexp"
    delimiter = "(?:$|\n)"
    decoder = "StatsdDecoder"

    [StatsdDecoder]
    type = "SandboxDecoder"
    filename = "lua_decoders/statsd.lua"
    [StatsdDecoder.config]
    payload_keep = true

*Example Heka Message*

:Timestamp: 2015-01-07 14:55:14 +0000 UTC
:Type: statsd
:Hostname: test.example.com
:Pid: 0
:Uuid: 5b7a82ab-ac0b-4412-a7a1-ce9574bd5fbb
:Logger: UdpInput
:Payload: deploys.test.myservice:3|ms|@7
:EnvVersion: 
:Severity: 7
:Fields:
    | name:"Value" type:double value:3
    | name:"Metric" type:string value:"deploys.test.myservice"
    | name:"Sampling" type:double value:7
    | name:"Modifier" type:string value:"ms"

--]]

local metric_field   = read_config("metric_field") or "Metric"
local value_field    = read_config("value_field") or "Value"
local modifier_field = read_config("modifier_field") or "Modifier"
local sampling_field = read_config("sampling_field") or "Sampling"
local msg_type       = read_config("msg_type") or "statsd"
local payload_keep   = read_config("payload_keep")

local l = require 'lpeg'
l.locale(l)

local pipe      = l.P("|")
local integer   = l.P("-")^-1 * lpeg.digit^1
local double    = integer * ("." * integer)^0
local number    = double * (l.S("Ee") * integer)^0 / tonumber

local metric    = l.Cg((lpeg.alnum + lpeg.S("-._"))^1, "metric")
local value     = l.Cg(number, "value")
local modifier  = l.Cg(lpeg.S("gcms") * lpeg.P("s")^0, "modifier") * (1 - pipe)^0
local sampling  = pipe * "@" * lpeg.Cg(number, "sampling")

local grammar   = l.Ct(metric * ":" * value * pipe * modifier * sampling^0)

function process_message ()

  local msg = {
    Type   = msg_type,
    Fields = {}
  }

  local line = read_message("Payload")
  local fields = grammar:match(line)
  if not fields then return -1 end

  msg.Fields[metric_field]   = fields.metric
  msg.Fields[value_field]    = fields.value
  msg.Fields[modifier_field] = fields.modifier
  msg.Fields[sampling_field] = fields.sampling

  if payload_keep then msg.Payload = line end

  inject_message(msg)
  return 0
end
