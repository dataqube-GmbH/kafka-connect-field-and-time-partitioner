### Kafka Connect Field and Time Based Partitioner

- Partition initially by a custom field and then by time.
- It extends **[TimeBasedPartitioner](https://github.com/confluentinc/kafka-connect-storage-common/blob/master/partitioner/src/main/java/io/confluent/connect/storage/partitioner/TimeBasedPartitioner.java)**, so any existing time based partition config should be fine.
- In order to make it work, set 
    - `partitioner.class=de.dataqube.kafka.connect.FieldAndTimeBasedPartitioner` 
    - `partition.field=<custom field in your record>`

Example JSON config:
```
{
    "name": "connect-s3-sink-connector",
    "config": {
        "topics": "events",
        "connector.class": "io.confluent.connect.s3.S3SinkConnector",
        "tasks.max" : 4,
        "flush.size": 100,
        "rotate.schedule.interval.ms": "-1",
        "rotate.interval.ms": "-1",
        "s3.region" : "eu-west-1",
        "s3.bucket.name" : "byob-raw",
        "s3.compression.type": "gzip",
        "topics.dir": "topics",
        "storage.class" : "io.confluent.connect.s3.storage.S3Storage",
        "partitioner.class": "com.canelmas.kafka.connect.FieldAndTimeBasedPartitioner",
        "partition.duration.ms" : "3600000",
        "path.format": "YYYY-MM-dd",
        "locale" : "US",
        "timezone" : "UTC",
        "schema.compatibility": "NONE",
        "format.class" : "io.confluent.connect.s3.format.json.JsonFormat",
        "timestamp.extractor": "Record",
        "partition.field" : "appId"
}
```