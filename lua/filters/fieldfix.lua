-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Performs some basic mutations on message Fields - add if not present, override,
remove, rename and parse.  New messages will be emitted with a new Type.

The Parse function can be used to extract field data embedded in another field,
ie; in a StatsD bucket name.  See Config for more information.

Config:

- msg_type (string, optional, default "fieldfix")
    String to set as the new message Type (which will also have
    'heka.sandbox.' automatically and unavoidably prefixed)

- payload_keep (bool, default false)
    If true, maintain the original Payloads in the new messages.

- fields_if_missing (string, optional)
    Space delimited string of Fields to supplement (ie; add if they're not
    already present).  Field name and value should be delimited with a '='.

- fields_override (string, optional)
    Space delimited string of Fields to always ensure exists, overriding
    existing values if necessary.  Field name and value should be delimited
    with a '='.

- fields_remove (string, optional)
    Space delimited string of Fields to remove.  Only the field name is
    required.

- fields_rename (string, optional)
    Space delimited string of Fields to rename.  Old and new name should be
    delimited with a '='.

- fields_parse (string, optional)
    Space delimited string of Fields to parse for embedded fields.

- fields_parse_key_delimiter (string, optional, default '._k_')
    String to demarcate keys embedded in the Field name

- fields_parse_value_delimiter (string, optional, default '._v_')
    String to separate values from keys embedded in the Field name

- fields_parse_prefix (string, optional)
    String to prepend to extracted Fields

*Example Heka Configuration*

.. code-block:: ini

    [FieldFixFilter]
    type = "SandboxFilter"
    filename = "lua_filters/fieldfix.lua"
    preserve_data = false

    [FieldFixFilter.config]
    fields_if_missing = "source=myhost target=remotehost"
    fields_override = "cluster=wibble"
    fields_remove = "product"

* Embedded Field parsing
You're using an existing client library/protocol/input/decoder and you'd like
to send additional data with an event.
By using the 'fields_parse' option, we can extract additional delimited KV
information from one field into their own Heka Fields for further
filtering/encoding/etc.

For example, Heka already has a hypothetical field "bucket_name".  You're able
to add additional information to the string from the client:

    | name:"bucket_name" type:string value:"deploys._k_dc._v_nyc._k_product._v_wibble"

With a config of 'fields_parse = "bucket_name"', this filter would generate the
following Fields:

:Fields:
    | name:"bucket_name" type:string value:"deploys"
    | name:"product" type:string value:"wibble"
    | name:"dc" type:string value:"nyc"

This becomes useful when transforming aggregated StatsD data for use by a tool
which supports the concept of 'tags' (such as OpenTSDB), for example the
following payload:

  deploys._k_product._v_dongles._k_dc._v_nyc._v_:3|ms|@7

The trailing fields_parse_value_delimiter (defaults to "._v_") plays a special
role here.  When consuming the last embedded value, the field_parse function
will look either to the end of the string, or until it hits a second value
delimiter.
Anything after that second value delimiter is appended to the leading text
("deploys" in this case).  This is useful when using the StatsD aggregator,
which suffixes a string to the bucket name (to denote the stat type, ".mean",
".sum", ".upper" etc.).

--]]

require "string"
require "lpeg"

local msg_type     = read_config("msg_type") or "fieldfix"
local payload_keep = read_config("payload_keep")
local add_str      = read_config("fields_if_missing") or ""
local override_str = read_config("fields_override") or ""
local remove_str   = read_config("fields_remove") or ""
local rename_str   = read_config("fields_rename") or ""
local parse_str    = read_config("fields_parse") or ""

local tag_key_delimiter   = read_config("fields_parse_key_delimiter") or "._k_"
local tag_value_delimiter = read_config("fields_parse_value_delimiter") or "._v_"
local tag_prefix          = read_config("fields_parse_prefix") or ""

-- convert a space-delimited string into a table of kv's, 
-- either splitting each token on '=', or setting the value
-- to 'true'
local function create_table(str)
  local t = {}
  if str:len() > 0 then
    for f in str:gmatch("[%S]+") do
      local k, v = f:match("([%S]+)=([%S]+)")
      if k ~= nil then
        t[k] = v
      else
        t[f] = true
      end
    end
  end
  return t
end

local function add_tag_prefix(tag)
  if tag_prefix then
    return tag_prefix .. tag
  end
  return tag
end

-- build tables
local add      = create_table(add_str)
local remove   = create_table(remove_str)
local override = create_table(override_str)
local rename   = create_table(rename_str)
local parse    = create_table(parse_str)

lpeg.locale(lpeg)

-- grammar for parsing embedded fields
local tkd   = lpeg.P(tag_key_delimiter)
local tvd   = lpeg.P(tag_value_delimiter)
local elem  = lpeg.C((1 - (tkd + tvd))^1)
local orig_name = lpeg.Cg((1 - tkd)^1, "orig_name")
local pair  = lpeg.Cg(elem / add_tag_prefix * tvd * elem) * tkd^-1
local tags  = (tkd * lpeg.Cg(lpeg.Cf(lpeg.Ct("") * pair^0, rawset), "tags"))^0
local trail = (tvd * lpeg.Cg((lpeg.alnum + lpeg.S("-._/"))^0, "trail"))^0
local grammar = lpeg.Ct(orig_name * tags * trail)


function process_message ()

    local message = {
      Timestamp  = read_message("Timestamp"),
      Type       = msg_type,
      EnvVersion = read_message("EnvVersion"),
      Fields     = {}
    }
    if payload_keep then message.Payload = read_message("Payload") end

    while true do
        local typ, name, value, representation, count = read_next_field()
        if not typ then break end

        -- Parse any Field values
        if parse[name] then
          local split = grammar:match(value)
          value = split.orig_name

          if split.trail ~= nil then
            value = value .. split.trail
          end

          if type(split.tags) == "table" then
            message.Fields = split.tags
          end
        end

        -- Fields to remove
        if remove[name] then 
        -- Fields to rename
        elseif rename[name] then
          message.Fields[rename[name]] = value
        -- Add untouched
        else
          message.Fields[name] = value
        end
    end

    -- Fields to supplement
    for k,v in pairs(add) do
      if not message.Fields[k] then
        message.Fields[k] = v
      end
    end

    -- Fields to override
    for k,v in pairs(override) do
      message.Fields[k] = v
    end

    inject_message(message)
    return 0
end

function timer_event(ns)
end
