/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2014
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Kieren Hynd <kieren@ticketmaster.com)
#
# ***** END LICENSE BLOCK *****/

package opentsdb

import (
	"bytes"
	"fmt"
	"github.com/mozilla-services/heka/pipeline"
	"os"
	"strings"
	"time"
)

// OpenTsdbRawEncoder generates a 'raw', line-based format of a message
// suitable for ingest into OpenTSDB over TCP.
type OpenTsdbRawEncoder struct {
	config   *OpenTsdbRawEncoderConfig
	hostname string
}

type OpenTsdbRawEncoderConfig struct {
	// String to demarcate embedded tag keys in the metric name
	TagNamePrefix string `toml:"tagname_prefix"`
	// String to demarcate embedded tag values in the metric name, defaults to '.'
	TagValuePrefix string `toml:"tagvalue_prefix"`
	// Base metric timestamp on either message Timestamp or "now"
	TsFromMessage bool `toml:"ts_from_message"`
	// Add any Fields with TagNamePrefix as tags
	FieldsToTags bool `toml:"fields_to_tags"`
	// Add a host= tag (with value of os.Hostname) if missing
	AddHostnameIfMissing bool `toml:"add_hostname_if_missing"`
}

func (oe *OpenTsdbRawEncoder) ConfigStruct() interface{} {
	return &OpenTsdbRawEncoderConfig{
		AddHostnameIfMissing: true,
		TsFromMessage:        true,
		FieldsToTags:         true,
	}
}

func (oe *OpenTsdbRawEncoder) Init(config interface{}) (err error) {
	oe.config = config.(*OpenTsdbRawEncoderConfig)
	oe.hostname, _ = os.Hostname()
	// We need to split a value from the key somehow, default to '.'
	if oe.config.TagNamePrefix != "" && oe.config.TagValuePrefix == "" {
		oe.config.TagValuePrefix = "."
	}

	return
}

func (oe *OpenTsdbRawEncoder) Encode(pack *pipeline.PipelinePack) (output []byte, err error) {

	buf := new(bytes.Buffer)

	metric, ok := pack.Message.GetFieldValue("Metric")
	if !ok {
		err = fmt.Errorf("Unable to find Field[Metric] in message")
		return nil, err
	}

	buf.WriteString("put ")

	var tags []string
	// if we're looking for dynamic field data embedded in the metric name...
	if oe.config.TagNamePrefix != "" {
		metric_parts := strings.Split(metric.(string), oe.config.TagNamePrefix)
		// write the metric name stripped of embedded tags
		buf.WriteString(metric_parts[0])
		// everything else will be embedded tag data
		tags = metric_parts[1:]
	} else {
		// just use the whole metric name
		buf.WriteString(fmt.Sprint(metric))
	}
	buf.WriteString(" ")

	// timestamp
	var timestamp time.Time
	if oe.config.TsFromMessage {
		timestamp = time.Unix(0, pack.Message.GetTimestamp()).UTC()
	} else {
		timestamp = time.Now()
	}
	buf.WriteString(fmt.Sprint(timestamp.Unix()))
	buf.WriteString(" ")

	// value
	value, ok := pack.Message.GetFieldValue("Value")
	if !ok {
		err = fmt.Errorf("Unable to find Field[Value] field in message")
		return nil, err
	}
	buf.WriteString(fmt.Sprint(value))

	// tags
	var seenHostTag bool
	// start with any tags that were embedded in the metric name
	for _, tag := range tags {
		kv := strings.SplitN(tag, oe.config.TagValuePrefix, 2)
		if len(kv) == 2 && kv[0] != "" && kv[1] != "" {
			if strings.ToLower(kv[0]) == "host" {
				seenHostTag = true
			}
			buf.WriteString(fmt.Sprintf("%s=%s", kv[0], kv[1]))
		}
	}

	// add any tags from dynamic Message fields that have the TagNamePrefix
	if oe.config.FieldsToTags {
		fields := pack.Message.GetFields()
		for _, field := range fields {
			k := field.GetName()
			if strings.HasPrefix(k, oe.config.TagNamePrefix) {
				if k == "Metric" || k == "Value" {
					continue
				}
				k = strings.TrimLeft(k, oe.config.TagNamePrefix)
				if k == "host" {
					seenHostTag = true
				}
				buf.WriteString(fmt.Sprintf(" %s=%v", k, field.GetValue()))
			}
		}
	}

	if !seenHostTag && oe.config.AddHostnameIfMissing && oe.hostname != "" {
		buf.WriteString(fmt.Sprintf(" host=%s", oe.hostname))
	}

	buf.WriteString("\n")
	return buf.Bytes(), nil
}

func init() {
	pipeline.RegisterPlugin("OpenTsdbRawEncoder", func() interface{} {
		return new(OpenTsdbRawEncoder)
	})
}
