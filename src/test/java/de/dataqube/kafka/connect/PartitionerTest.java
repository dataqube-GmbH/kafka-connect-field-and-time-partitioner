package de.dataqube.kafka.connect;

import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.partitioner.TimestampExtractor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PartitionerTest {

    private Map<String,Object> getTestConfig() {
        Map<String,Object> configs = new HashMap<>();
        configs.put("partition.duration.ms", 3600000L);
        configs.put("path.format", "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH");
        configs.put("locale", "de-DE");
        configs.put("timezone", "UTC");
        configs.put("directory.delim", "/");
        configs.put("timestamp.extractor", "Wallclock");
        return configs;
    }

    @Test
    public void testTimestampExtractorIsSet() {
        Map<String,Object> configs = getTestConfig();
        configs.put("partition.field", "payload.metadata.company");
        FieldAndTimeBasedPartitioner<String> partitioner = new FieldAndTimeBasedPartitioner<String>();
        partitioner.configure(configs);

        TimestampExtractor timestampExtractor = ((TimeBasedPartitioner) partitioner).getTimestampExtractor();
        Assert.assertNotNull(timestampExtractor);
    }

    @Test
    public void testSettingMultipleFieldNames() {
        Map<String,Object> configs = getTestConfig();
        configs.put("partition.field", "my.field1,my.field2");
        FieldAndTimeBasedPartitioner<String> partitioner = new FieldAndTimeBasedPartitioner<String>();
        partitioner.configure(configs);

        Schema innerSchema = SchemaBuilder.struct().name("inner")
                .field("field1", Schema.STRING_SCHEMA)
                .field("field2", Schema.INT32_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct().name("outer").field("my", innerSchema);

        Struct innerStruct = new Struct(innerSchema)
                .put("field1", "myvalue1")
                .put("field2", 2);

        Struct struct = new Struct(schema).put("my", innerStruct);

        SinkRecord record = new SinkRecord("topic", 0, null, null, schema, struct, 0);

        String partition = partitioner.encodePartition(record);
        Assert.assertTrue(partition.startsWith("field1=myvalue1/field2=2/"));
    }


}
