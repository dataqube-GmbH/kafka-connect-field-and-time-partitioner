package de.dataqube.kafka.connect;

import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.partitioner.TimestampExtractor;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class PartitionerTest {

    @Test
    public void testInstaceOf() {
        Map<String,Object> configs = new HashMap<>();
        configs.put("partition.field", "payload.metadata.company");
        configs.put("timestamp.extractor", "RecordField");
        configs.put("timestamp.field", "timestamp");
        FieldAndTimeBasedPartitioner<String> partitioner = new FieldAndTimeBasedPartitioner<String>();
        partitioner.init(3600000L, "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
                Locale.getDefault(), DateTimeZone.getDefault(), configs);

        TimestampExtractor timestampExtractor1 = partitioner.getTimestampExtractor();

        TimestampExtractor timestampExtractor2 = partitioner instanceof TimeBasedPartitioner
                ? ((TimeBasedPartitioner) partitioner).getTimestampExtractor()
                : null;
        Assert.assertNotNull(timestampExtractor1);
        Assert.assertNotNull(timestampExtractor2);
    }
}
