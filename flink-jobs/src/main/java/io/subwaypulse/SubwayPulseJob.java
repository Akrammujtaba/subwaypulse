package io.subwaypulse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.subwaypulse.avro.TripEvent;
import io.subwaypulse.avro.TripEventType;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * The streaming job.
 *
 * <pre>
 *   subway.trip_events (Avro) ──► ArrivalDetector (by trip) ──► HeadwayMonitor (by route+stop) ──► headways
 *          │                              │                         ▲          │
 *          ▼                              ▼                         │          ▼
 *   bronze.trip_events            silver.arrivals     schedule.headways    incidents ──► gold.incident_events
 *                                                      (broadcast)                  └──► subway.incidents (Kafka)
 * </pre>
 *
 * Event time is the feed timestamp. Iceberg writes are exactly-once (committed on
 * checkpoint). Incident alerts go to Kafka at-least-once so they don't wait for a
 * checkpoint; each carries a deterministic id, so a consumer can drop repeats.
 */
public class SubwayPulseJob {

    static String env(String key, String dflt) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? dflt : v;
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP", "kafka:9092");
        String registry = env("SCHEMA_REGISTRY_URL", "http://schema-registry:8085");
        long outOfOrderMs = Long.parseLong(env("OUT_OF_ORDER_SECONDS", "45")) * 1000;
        long releaseDelayMs = Long.parseLong(env("RELEASE_DELAY_SECONDS", "60")) * 1000;
        long checkpointMs = Long.parseLong(env("CHECKPOINT_SECONDS", "30")) * 1000;
        long outageAfterMs = Long.parseLong(env("OUTAGE_AFTER_SECONDS", "120")) * 1000;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointMs, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cc = env.getCheckpointConfig();
        cc.setMinPauseBetweenCheckpoints(checkpointMs / 2);
        cc.setCheckpointTimeout(Duration.ofMinutes(5).toMillis());
        cc.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // ---------- trip events ----------
        KafkaSource<TripEvent> tripSource = KafkaSource.<TripEvent>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(env("TRIP_TOPIC", "subway.trip_events"))
                .setGroupId("subwaypulse-flink")
                // Resume from offsets committed on the last checkpoint if the job is
                // started fresh; start from the beginning the very first time.
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new RegistryAvroDeserializer(registry))
                .build();

        // Watermarks come from every record, heartbeats included, so a partition with no
        // changed trips still moves event time forward.
        WatermarkStrategy<TripEvent> wm = WatermarkStrategy
                .<TripEvent>forBoundedOutOfOrderness(Duration.ofMillis(outOfOrderMs))
                .withTimestampAssigner((e, ts) -> e.getFeedTs().toEpochMilli())
                .withIdleness(Duration.ofMinutes(2));

        DataStream<TripEvent> allEvents = env.fromSource(tripSource, wm, "trip-events")
                .uid("trip-events-source");
        DataStream<TripEvent> tripEvents = allEvents
                .filter(e -> e.getEventType() != TripEventType.HEARTBEAT)
                .name("drop-heartbeats").uid("drop-heartbeats");

        // Heartbeats mark every successful fetch; a long silence between two is an outage.
        DataStream<Reference> outages = allEvents
                .filter(e -> e.getEventType() == TripEventType.HEARTBEAT)
                .name("heartbeats").uid("heartbeats")
                .keyBy(e -> e.getFeedId())
                .process(new OutageDetector(outageAfterMs))
                .name("outage-detector").uid("outage-detector");

        // ---------- schedule (broadcast) ----------
        KafkaSource<String> scheduleSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(env("SCHEDULE_TOPIC", "schedule.headways"))
                .setGroupId("subwaypulse-flink-schedule")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema(StandardCharsets.UTF_8))
                .build();

        // The schedule is reference data, not events: it must never hold back event time.
        WatermarkStrategy<String> neverBlocks = WatermarkStrategy.forGenerator(ctx -> new WatermarkGenerator<>() {
            @Override
            public void onEvent(String event, long ts, WatermarkOutput out) {}

            @Override
            public void onPeriodicEmit(WatermarkOutput out) {
                out.emitWatermark(new Watermark(Long.MAX_VALUE));
            }
        });

        // Schedule rows, outages, and "feed seen" markers share one broadcast input.
        BroadcastStream<Reference> reference = env.fromSource(scheduleSource, neverBlocks, "schedule")
                .uid("schedule-source")
                .map(new ParseSchedule()).name("parse-schedule").uid("parse-schedule")
                .filter(r -> r != null)
                .map(r -> Reference.of(r)).returns(Reference.class).name("schedule-ref").uid("schedule-ref")
                .union(outages)
                .broadcast(HeadwayMonitor.SCHEDULE, HeadwayMonitor.OUTAGES, HeadwayMonitor.LAST_SEEN);

        // ---------- processing ----------
        DataStream<Arrival> arrivals = tripEvents
                .keyBy(e -> e.getTripKey())
                .process(new ArrivalDetector())
                .name("arrival-detector").uid("arrival-detector");

        SingleOutputStreamOperator<Headway> headways = arrivals
                .keyBy(Arrival::lineStopKey)
                .connect(reference)
                .process(new HeadwayMonitor(releaseDelayMs))
                .name("headway-monitor").uid("headway-monitor");

        DataStream<Incident> incidents = headways.getSideOutput(HeadwayMonitor.INCIDENTS);

        // ---------- sinks ----------
        CatalogLoader catalog = catalogLoader();
        IcebergTables.createIfMissing(catalog);

        sink(tripEvents.map(IcebergTables::tripEvent), catalog, IcebergTables.TRIP_EVENTS,
                IcebergTables.TRIP_EVENTS_SCHEMA, "bronze-trip-events");
        sink(arrivals.map(IcebergTables::arrival), catalog, IcebergTables.ARRIVALS,
                IcebergTables.ARRIVALS_SCHEMA, "silver-arrivals");
        sink(headways.map(IcebergTables::headway), catalog, IcebergTables.HEADWAYS,
                IcebergTables.HEADWAYS_SCHEMA, "silver-headways");
        sink(incidents.map(IcebergTables::incident), catalog, IcebergTables.INCIDENT_EVENTS,
                IcebergTables.INCIDENT_EVENTS_SCHEMA, "gold-incident-events");

        KafkaSink<String> alerts = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(env("INCIDENT_TOPIC", "subway.incidents"))
                        .setValueSerializationSchema(new SimpleStringSchema(StandardCharsets.UTF_8))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        incidents.map(Incident::toJson).name("incident-json").uid("incident-json")
                .sinkTo(alerts).name("incident-alerts").uid("incident-alerts");

        env.execute("subwaypulse");
    }

    static void sink(DataStream<RowData> rows, CatalogLoader catalog, TableIdentifier id, Schema schema,
                     String uid) {
        // map() loses the RowData type information; set it explicitly for the sink.
        DataStream<RowData> typed = ((SingleOutputStreamOperator<RowData>) rows)
                .returns(InternalTypeInfo.of(FlinkSchemaUtil.convert(schema)))
                .name(uid + "-rows").uid(uid + "-rows");
        FlinkSink.forRowData(typed)
                .tableLoader(TableLoader.fromCatalog(catalog, id))
                .uidPrefix(uid)
                .append();
    }

    static CatalogLoader catalogLoader() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", env("ICEBERG_JDBC_URI", "jdbc:postgresql://postgres:5432/iceberg"));
        props.put("jdbc.user", env("ICEBERG_JDBC_USER", "subway"));
        props.put("jdbc.password", env("ICEBERG_JDBC_PASSWORD", "subway"));
        props.put("warehouse", env("ICEBERG_WAREHOUSE", "s3://lake/warehouse"));
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.endpoint", env("S3_ENDPOINT", "http://s3:8333"));
        props.put("s3.path-style-access", "true");
        props.put("client.region", env("AWS_REGION", "us-east-1"));
        return CatalogLoader.custom(env("ICEBERG_CATALOG", "lake"), props, new Configuration(),
                "org.apache.iceberg.jdbc.JdbcCatalog");
    }

    /** Parses one schedule.headways message. Bad rows are dropped, not fatal. */
    static class ParseSchedule implements MapFunction<String, ScheduleRow> {
        private static final ObjectMapper JSON = new ObjectMapper();

        @Override
        public ScheduleRow map(String value) {
            if (value == null) {
                return null;
            }
            try {
                JsonNode n = JSON.readTree(value);
                ScheduleRow r = new ScheduleRow();
                r.routeId = n.get("route_id").asText();
                r.stopId = n.get("stop_id").asText();
                r.localDate = n.get("local_date").asText();
                r.hour = n.get("hour").asInt();
                r.scheduledHeadwaySeconds = n.get("scheduled_headway_s").asInt();
                return r;
            } catch (Exception e) {
                return null;
            }
        }
    }
}
