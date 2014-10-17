/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2012
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Kieren Hynd <kieren@ticketmaster.com)
#
# ***** END LICENSE BLOCK *****/

package opentsdb

import (
	"fmt"
	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
	"strconv"
	"strings"
	"time"
)

// Decoder that expects OpenTSDB string format data in the message payload,
// converts that to identical stats data in the message fields, in the same
// format that a StatAccumInput w/ `emit_in_fields` set to true would use.
type OpenTsdbRawDecoder struct {
	runner DecoderRunner
	helper PluginHelper
	config *OpenTsdbRawDecoderConfig
}

type OpenTsdbRawDecoderConfig struct {
	// Prefix for any Fields derived from tags
	TagNamePrefix string `toml:"tagname_prefix"`
}

func (d *OpenTsdbRawDecoder) ConfigStruct() interface{} {
	return &OpenTsdbRawDecoderConfig{}
}

func (d *OpenTsdbRawDecoder) Init(config interface{}) error {
	d.config = config.(*OpenTsdbRawDecoderConfig)
	return nil
}

// Implement `WantsDecoderRunner`
func (d *OpenTsdbRawDecoder) SetDecoderRunner(dr DecoderRunner) {
	d.runner = dr
}

func (d *OpenTsdbRawDecoder) Decode(pack *PipelinePack) (packs []*PipelinePack,
	err error) {

	line := strings.Trim(pack.Message.GetPayload(), "\n")

	// Strip any leading 'put 's
	line = strings.TrimPrefix(line, "put ")

	// Should maybe do this on encoding/output instead
	if len(line) >= 1024 {
		err = fmt.Errorf("metric line exceeds 1KB: '%s'", line)
		return
	}

	// Break into fields
	fields := strings.Fields(line)

	// Sanity check.
	if len(fields) < 3 {
		err = fmt.Errorf("malformed metric line: '%s'", line)
		return
	}

	// Check timestamp validity.
	unixTime, err := strconv.Atoi(fields[1])
	if err != nil {
		err = fmt.Errorf("invalid timestamp: '%s'", line)
		return
	}
	pack.Message.SetTimestamp(time.Unix(int64(unixTime), 0).UnixNano())

	// Add metric to the main message.
	if err = d.addStatField(pack, "Metric", fields[0]); err != nil {
		return
	}

	// Check value validity.
	// OpenTSDB checks if value "looks like" an int before a float
	var value interface{}
	if value, err = strconv.ParseInt(fields[2], 10, 64); err != nil {
		if value, err = strconv.ParseFloat(fields[2], 64); err != nil {
			err = fmt.Errorf("invalid value: '%s'", line)
			return
		}
	}
	// Add value to the main message.
	if err = d.addStatField(pack, "Value", value); err != nil {
		return
	}

	// Add any tags
	for _, tag := range fields[3:] {
		x := strings.SplitN(tag, "=", 2)
		if err = d.addStatField(pack, d.config.TagNamePrefix+x[0], x[1]); err != nil {
			return
		}
	}

	packs = []*PipelinePack{pack}
	return
}

func (d *OpenTsdbRawDecoder) addStatField(pack *PipelinePack, name string,
	value interface{}) error {

	field, err := message.NewField(name, value, "")
	if err != nil {
		return fmt.Errorf("error adding field '%s': %s", name, err)
	}
	pack.Message.AddField(field)
	return nil
}

func init() {
	RegisterPlugin("OpenTsdbRawDecoder", func() interface{} {
		return new(OpenTsdbRawDecoder)
	})
}
