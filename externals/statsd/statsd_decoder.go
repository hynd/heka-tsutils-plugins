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
#   Ben Bangert (bbangert@mozilla.com)
#   Rob Miller (rmiller@mozilla.com)
#   Kieren Hynd <kieren@ticketmaster.com)
#
# ***** END LICENSE BLOCK *****/

package statsd

import (
	"fmt"
	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
	"strconv"
	"strings"
)

// Decoder that expects a single StatsD-formatted string in the message payload.
// Breaks the message into Fields for Metric, Value, Modifier and Sampling.
type StatsdDecoder struct {
	runner DecoderRunner
	helper PluginHelper
}

func (d *StatsdDecoder) Init(config interface{}) error {
	return nil
}

// Implement `WantsDecoderRunner`
func (d *StatsdDecoder) SetDecoderRunner(dr DecoderRunner) {
	d.runner = dr
}

func (d *StatsdDecoder) Decode(pack *PipelinePack) (packs []*PipelinePack, err error) {

	line := strings.Trim(pack.Message.GetPayload(), "\n")
	if len(line) == 0 {
		err = fmt.Errorf("empty stats line")
		return
	}

	// split the bucket name from the rest
	parts := strings.SplitN(line, ":", 2)
	if len(parts) != 2 {
		err = fmt.Errorf("failed to split on colon: '%s'", line)
		return
	}

	// set the metric name
	if err = d.addStatField(pack, "Metric", parts[0]); err != nil {
		return
	}

	// look for pipes - some types have 3 pipes
	parts = strings.SplitN(parts[1], "|", 3)
	if len(parts) < 2 {
		err = fmt.Errorf("not enough pipes: '%s'", line)
		return
	}
	value, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		err = fmt.Errorf("invalid value: '%s'", line)
		return
	}
	if err = d.addStatField(pack, "Value", value); err != nil {
		return
	}

	// check for valid modifiers
	switch parts[1] {
	case "g", "c", "ms", "s":
		if err = d.addStatField(pack, "Modifier", parts[1]); err != nil {
			return
		}
	default:
		err = fmt.Errorf("unknown metric type: '%s'", line)
		return
	}

	// add a @samplerate if it was given, otherwise default to 1
	rate := float64(1)
	if len(parts) == 3 && strings.HasPrefix(parts[2], "@") {
		rate, err = strconv.ParseFloat(parts[2][1:], 32)
		if err != nil {
			fmt.Errorf("coudn't parse sample rate: '%s'", line)
			return
		}
	}
	if err = d.addStatField(pack, "Sampling", rate); err != nil {
		return
	}

	pack.Message.SetType("statsd")
	packs = []*PipelinePack{pack}
	return
}

func (d *StatsdDecoder) addStatField(pack *PipelinePack, name string,
	value interface{}) error {

	field, err := message.NewField(name, value, "")
	if err != nil {
		return fmt.Errorf("error adding field '%s': %s", name, err)
	}
	pack.Message.AddField(field)
	return nil
}

func init() {
	RegisterPlugin("StatsdDecoder", func() interface{} {
		return new(StatsdDecoder)
	})
}
