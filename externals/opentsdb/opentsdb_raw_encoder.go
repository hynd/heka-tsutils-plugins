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
#   Kieren Hynd (kieren@ticketmaster.com)
#
# ***** END LICENSE BLOCK *****/

package opentsdb

import (
	"bytes"
	"fmt"
	"github.com/mozilla-services/heka/pipeline"
	"strings"
	"time"
)

type dedupe struct {
	data    []byte
	skipped bool
	ts      int64
	val     interface{}
}

// OpenTsdbRawEncoder generates a 'raw', line-based format of a message
// suitable for ingest into OpenTSDB over TCP.
type OpenTsdbRawEncoder struct {
	config       *OpenTsdbRawEncoderConfig
	dedupeBuffer map[string]dedupe
	missingTags  map[string]string
	overrideTags map[string]string
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
	// Maximum window size (seconds) for dedupe
	DedupeFlush int64 `toml:"dedupe_window"`
	// Array of static tags to add if missing
	AddTagsIfMissing []string `toml:"tags_if_missing"`
	// Array of static tags to override unconditionally
	AddTagsOverride []string `toml:"tags_override"`
}

func (oe *OpenTsdbRawEncoder) ConfigStruct() interface{} {
	return &OpenTsdbRawEncoderConfig{
		TsFromMessage: true,
		FieldsToTags:  true,
	}
}

func (oe *OpenTsdbRawEncoder) Init(config interface{}) (err error) {
	oe.config = config.(*OpenTsdbRawEncoderConfig)
	oe.dedupeBuffer = make(map[string]dedupe)
	oe.missingTags = make(map[string]string)
	oe.overrideTags = make(map[string]string)
	// We need to split a value from the key somehow, default to '.'
	if oe.config.TagNamePrefix != "" && oe.config.TagValuePrefix == "" {
		oe.config.TagValuePrefix = "."
	}

	if len(oe.config.AddTagsIfMissing) > 0 {
		for _, t := range oe.config.AddTagsIfMissing {
			kv := strings.SplitN(t, "=", 2)
			if len(kv) == 2 && kv[0] != "" && kv[1] != "" {
				oe.missingTags[kv[0]] = kv[1]
			}
		}
	}
	if len(oe.config.AddTagsOverride) > 0 {
		for _, t := range oe.config.AddTagsOverride {
			kv := strings.SplitN(t, "=", 2)
			if len(kv) == 2 && kv[0] != "" && kv[1] != "" {
				oe.overrideTags[kv[0]] = kv[1]
			}
		}
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
	tagMap := make(map[string]interface{})
	var tagKeys []string
	// start with any tags that were embedded in the metric name
	for _, tag := range tags {
		kv := strings.SplitN(tag, oe.config.TagValuePrefix, 2)
		if len(kv) == 2 && kv[0] != "" && kv[1] != "" {
			tagMap[kv[0]] = kv[1]
			tagKeys = append(tagKeys, kv[0])
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
				tagMap[k] = field.GetValue()
				tagKeys = append(tagKeys, k)
			}
		}
	}

	// add any tags if they're missing
	for k, v := range oe.missingTags {
		if _, ok := tagMap[k]; !ok {
			tagKeys = append(tagKeys, k)
			tagMap[k] = v
		}
	}

	// override any tags unconditionally
	for k, v := range oe.overrideTags {
		if _, ok := tagMap[k]; !ok {
			tagKeys = append(tagKeys, k)
		}
		tagMap[k] = v
	}

	// build the final tag string
	tagString := new(bytes.Buffer)
	for _, k := range tagKeys {
		tagString.WriteString(fmt.Sprintf(" %s=%v", k, tagMap[k]))
	}

	buf.Write(tagString.Bytes())
	buf.WriteString("\n")

	// dedupe
	var previous []byte
	if oe.config.DedupeFlush > 0 {
		bufkey := fmt.Sprintf("%s:%s", metric, tagString)

		if _, ok := oe.dedupeBuffer[bufkey]; ok {

			// if we've already seen the value, add it to the buffer
			if oe.dedupeBuffer[bufkey].val == value &&
				(timestamp.UnixNano()-oe.dedupeBuffer[bufkey].ts < oe.config.DedupeFlush*1e9) {

				oe.dedupeBuffer[bufkey] = dedupe{data: buf.Bytes(), skipped: true, val: value, ts: oe.dedupeBuffer[bufkey].ts}
				return nil, nil
			}

			// if the value's changed, and we've skipped it before (or it's been > the flush interval)
			// return the stored data point, and the current one
			if (oe.dedupeBuffer[bufkey].skipped ||
				(oe.dedupeBuffer[bufkey].skipped && timestamp.UnixNano()-oe.dedupeBuffer[bufkey].ts >= oe.config.DedupeFlush*1e9)) &&
				oe.dedupeBuffer[bufkey].val != value {

				previous = oe.dedupeBuffer[bufkey].data
			}
		}
		// track the last data point
		oe.dedupeBuffer[bufkey] = dedupe{data: buf.Bytes(), val: value, ts: timestamp.UnixNano()}
	}

	return append(previous, buf.Bytes()...), nil
}

func init() {
	pipeline.RegisterPlugin("OpenTsdbRawEncoder", func() interface{} {
		return new(OpenTsdbRawEncoder)
	})
}
