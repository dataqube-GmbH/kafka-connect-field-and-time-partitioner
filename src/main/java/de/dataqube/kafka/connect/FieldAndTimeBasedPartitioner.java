/*
 * Copyright (C) 2019 Can Elmas <canelm@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.dataqube.kafka.connect;

import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.partitioner.TimestampExtractor;
import io.confluent.connect.storage.util.DataUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;

public final class FieldAndTimeBasedPartitioner<T> extends TimeBasedPartitioner<T> {

    private static final Logger log = LoggerFactory.getLogger(FieldAndTimeBasedPartitioner.class);

    private long partitionDurationMs;
    private DateTimeFormatter formatter;
    private TimestampExtractor timestampExtractor;

    private PartitionFieldExtractor partitionFieldExtractor;

    protected void init(long partitionDurationMs, String pathFormat, Locale locale, DateTimeZone timeZone, Map<String, Object> config) {

        this.delim = (String)config.get("directory.delim");
        this.partitionDurationMs = partitionDurationMs;

        try {

            this.formatter = getDateTimeFormatter(pathFormat, timeZone).withLocale(locale);
            this.timestampExtractor = this.newTimestampExtractor((String)config.get("timestamp.extractor"));
            this.timestampExtractor.configure(config);
            String partitionFields = (String)config.get("partition.fields");
            if (partitionFields == null) {
                // fallback (from previous version)
                partitionFields = (String) config.get("partition.field");
            }
            String partitionNames = (String)config.get("partition.names");

            this.partitionFieldExtractor = new PartitionFieldExtractor(partitionFields, partitionNames);

        } catch (IllegalArgumentException e) {

            ConfigException ce = new ConfigException("path.format", pathFormat, e.getMessage());
            ce.initCause(e);
            throw ce;

        }
    }

    private static DateTimeFormatter getDateTimeFormatter(String str, DateTimeZone timeZone) {
        return DateTimeFormat.forPattern(str).withZone(timeZone);
    }

    @Override
    public TimestampExtractor getTimestampExtractor() {
        return this.timestampExtractor;
    }

    public static long getPartition(long timeGranularityMs, long timestamp, DateTimeZone timeZone) {

        long adjustedTimestamp = timeZone.convertUTCToLocal(timestamp);
        long partitionedTime = adjustedTimestamp / timeGranularityMs * timeGranularityMs;

        return timeZone.convertLocalToUTC(partitionedTime, false);

    }

    public String encodePartition(SinkRecord sinkRecord, long nowInMillis) {

        final Long timestamp = this.timestampExtractor.extract(sinkRecord, nowInMillis);
        final String partitionFields = String.join(delim, this.partitionFieldExtractor.extract(sinkRecord));

        return this.encodedPartitionForFieldAndTime(sinkRecord, timestamp, partitionFields);

    }

    public String encodePartition(SinkRecord sinkRecord) {

        final Long timestamp = this.timestampExtractor.extract(sinkRecord);
        final String partitionFieldValues = String.join(this.delim, this.partitionFieldExtractor.extract(sinkRecord));

        return encodedPartitionForFieldAndTime(sinkRecord, timestamp, partitionFieldValues);

    }

    private String encodedPartitionForFieldAndTime(SinkRecord sinkRecord, Long timestamp, String partitionField) {

        if (timestamp == null) {

            String msg = "Unable to determine timestamp using timestamp.extractor " + this.timestampExtractor.getClass().getName() + " for record: " + sinkRecord;
            log.error(msg);
            throw new ConnectException(msg);

        } else if (partitionField == null) {

            String msg = "Unable to determine partition field using partition.field '" + partitionField  + "' for record: " + sinkRecord;
            log.error(msg);
            throw new ConnectException(msg);

        }  else {

            DateTime bucket = new DateTime(getPartition(this.partitionDurationMs, timestamp, this.formatter.getZone()));
            return partitionField + this.delim + bucket.toString(this.formatter);

        }
    }

    static class PartitionFieldExtractor {

        private String[] fieldNames;
        private String[] partitionNames;
        private Boolean includeName;
        private String[] fieldNameRepr;

        PartitionFieldExtractor(String fieldNames, String partitionNames) {
            this(fieldNames, partitionNames, true);
        }

        PartitionFieldExtractor(String fieldNames, String partitionNames, Boolean includeName) {
            try {
                this.fieldNames = fieldNames.split(",");
                if(partitionNames != null) {
                    this.partitionNames = partitionNames.split(",");
                }
                this.includeName = includeName;
                this.fieldNameRepr = new String[this.fieldNames.length];
                for(int i=0; i<this.fieldNames.length; ++i) {
                    String[] splitted = this.fieldNames[i].split("\\.");
                    this.fieldNameRepr[i] = splitted[splitted.length - 1];
                }

            } catch(Exception e) {
                ConfigException ce = new ConfigException("partition.field", fieldNames, e.getMessage());
                ce.initCause(e);
                throw ce;
            }
        }

        String[] extract(ConnectRecord<?> record) {

            Object value = record.value();
            String[] extracted = new String[this.fieldNames.length];

            for(int i=0; i<this.fieldNames.length; ++i) {
                String fieldName = this.fieldNames[i];
                String fieldNameRepr = this.partitionNames != null ? this.partitionNames[i] : this.fieldNameRepr[i];

                if (value instanceof Struct) {
                    final Object field = DataUtils.getNestedFieldValue(value, fieldName);
                    extracted[i] = getRepr(field.toString(), fieldNameRepr);
                } else if (value instanceof Map) {
                    extracted[i] =  getRepr(DataUtils.getNestedFieldValue(value, fieldName).toString(), fieldNameRepr);
                } else {
                    FieldAndTimeBasedPartitioner.log.error("Value is not of Struct or Map type.");
                    throw new PartitionException("Error encoding partition.");
                }
            }
            return extracted;
        }

        private String getRepr(String value, String fieldNameRepr) {
            return this.includeName ? String.format("%s=%s", fieldNameRepr, value) : value;
        }

    }
}
