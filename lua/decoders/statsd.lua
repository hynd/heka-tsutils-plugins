-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Parses a StatsD message into Heka Fields.
Intended to work with Heka's vanilla UdpInput, aggregated by a Filter.

Config:
- metric_field (string, optional, default "Metric")
    Field name to use for the bucket name

- value_field (string, optional, default "Value")
    Field name to use for the stat value

- modifier_field (string, optional, default "Modifier")
    Field name to use for the stat's "modifier" (ie; "c", "ms" etc)

- sampling_field (string, optional, default "Sampling")
    Field name to use for the stat's sampling rate (value defaults to 1)

- msg_type (string, optional, defaults to existing Type)
    Sets the message 'Type' header to the specified value.


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
:Type: NetworkInput
:Hostname:
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
local msg_type       = read_config("msg_type")

local l = require 'lpeg'
l.locale(l)

local pipe      = l.P("|")
local integer   = l.P("-")^-1 * l.digit^1
local double    = integer * ("." * integer)^0
local number    = double * (l.S("Ee") * integer)^0 / tonumber

local metric    = l.Cg((l.alnum + l.S("-._"))^1, "metric")
local value     = l.Cg(number, "value")
local modifier  = l.Cg(l.S("gcms") * l.P("s")^0, "modifier") * (1 - pipe)^0
local sampling  = pipe * "@" * l.Cg(number, "sampling")

local grammar   = l.Ct(metric * ":" * value * pipe * modifier * sampling^0)

function process_message ()

  local line = read_message("Payload")
  local fields = grammar:match(line)
  if not fields then return -1 end

  write_message("Fields["..metric_field.."]", fields.metric)
  write_message("Fields["..value_field.."]", fields.value)
  write_message("Fields["..modifier_field.."]", fields.modifier)

  -- set the sampling rate field (if there was one)
  if fields.sampling then
    write_message("Fields["..sampling_field.."]", fields.sampling)
  else
    write_message("Fields["..sampling_field.."]", 1)
  end

  -- optionally overwrite Type
  if msg_type then
    write_message("Type", msg_type)
  end

  return 0
end
