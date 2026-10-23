package io.subwaypulse;

import io.subwaypulse.avro.StopPrediction;
import io.subwaypulse.avro.TripEvent;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.types.Types;

/** Table definitions and RowData converters for everything the job writes to Iceberg. */
public final class IcebergTables {
    private IcebergTables() {}

    public static final TableIdentifier TRIP_EVENTS = TableIdentifier.of("bronze", "trip_events");
    public static final TableIdentifier ARRIVALS = TableIdentifier.of("silver", "arrivals");
    public static final TableIdentifier HEADWAYS = TableIdentifier.of("silver", "headways");
    public static final TableIdentifier INCIDENT_EVENTS = TableIdentifier.of("gold", "incident_events");

    static final Schema TRIP_EVENTS_SCHEMA = new Schema(
            Types.NestedField.required(1, "event_type", Types.StringType.get()),
            Types.NestedField.required(2, "feed_id", Types.StringType.get()),
            Types.NestedField.required(3, "feed_ts", Types.TimestampType.withZone()),
            Types.NestedField.required(4, "fetched_at", Types.TimestampType.withZone()),
            Types.NestedField.optional(5, "trip_key", Types.StringType.get()),
            Types.NestedField.optional(6, "trip_id", Types.StringType.get()),
            Types.NestedField.optional(7, "start_date", Types.StringType.get()),
            Types.NestedField.optional(8, "route_id", Types.StringType.get()),
            Types.NestedField.optional(9, "direction", Types.StringType.get()),
            Types.NestedField.optional(10, "vehicle_status", Types.StringType.get()),
            Types.NestedField.optional(11, "vehicle_stop_id", Types.StringType.get()),
            Types.NestedField.optional(12, "vehicle_ts", Types.TimestampType.withZone()),
            Types.NestedField.optional(13, "stops", Types.ListType.ofRequired(14, Types.StructType.of(
                    Types.NestedField.required(15, "stop_id", Types.StringType.get()),
                    Types.NestedField.optional(16, "arrival_ts", Types.TimestampType.withZone()),
                    Types.NestedField.optional(17, "departure_ts", Types.TimestampType.withZone())))));

    static final Schema ARRIVALS_SCHEMA = new Schema(
            Types.NestedField.required(1, "trip_key", Types.StringType.get()),
            Types.NestedField.required(2, "feed_id", Types.StringType.get()),
            Types.NestedField.required(3, "route_id", Types.StringType.get()),
            Types.NestedField.optional(4, "direction", Types.StringType.get()),
            Types.NestedField.required(5, "stop_id", Types.StringType.get()),
            Types.NestedField.required(6, "arrival_ts", Types.TimestampType.withZone()),
            Types.NestedField.required(7, "observed_at", Types.TimestampType.withZone()),
            Types.NestedField.required(8, "detected_at", Types.TimestampType.withZone()));

    static final Schema HEADWAYS_SCHEMA = new Schema(
            Types.NestedField.required(1, "route_id", Types.StringType.get()),
            Types.NestedField.optional(2, "direction", Types.StringType.get()),
            Types.NestedField.required(3, "stop_id", Types.StringType.get()),
            Types.NestedField.required(4, "trip_key", Types.StringType.get()),
            Types.NestedField.required(5, "prev_trip_key", Types.StringType.get()),
            Types.NestedField.required(6, "arrival_ts", Types.TimestampType.withZone()),
            Types.NestedField.required(7, "prev_arrival_ts", Types.TimestampType.withZone()),
            Types.NestedField.required(8, "headway_s", Types.LongType.get()),
            Types.NestedField.optional(9, "scheduled_headway_s", Types.IntegerType.get()),
            Types.NestedField.required(10, "status", Types.StringType.get()),
            Types.NestedField.required(11, "detected_at", Types.TimestampType.withZone()));

    static final Schema INCIDENT_EVENTS_SCHEMA = new Schema(
            Types.NestedField.required(1, "incident_id", Types.StringType.get()),
            Types.NestedField.required(2, "event_type", Types.StringType.get()),
            Types.NestedField.required(3, "route_id", Types.StringType.get()),
            Types.NestedField.optional(4, "direction", Types.StringType.get()),
            Types.NestedField.required(5, "stop_id", Types.StringType.get()),
            Types.NestedField.required(6, "prev_arrival_ts", Types.TimestampType.withZone()),
            Types.NestedField.required(7, "prev_trip_key", Types.StringType.get()),
            Types.NestedField.optional(8, "arrival_ts", Types.TimestampType.withZone()),
            Types.NestedField.optional(9, "trip_key", Types.StringType.get()),
            Types.NestedField.required(10, "event_ts", Types.TimestampType.withZone()),
            Types.NestedField.optional(11, "headway_s", Types.LongType.get()),
            Types.NestedField.required(12, "scheduled_headway_s", Types.IntegerType.get()),
            Types.NestedField.required(13, "threshold_s", Types.IntegerType.get()),
            Types.NestedField.required(14, "detected_at", Types.TimestampType.withZone()));

    /** Table properties chosen for a table that gets a small commit every checkpoint. */
    static Map<String, String> properties() {
        Map<String, String> p = new HashMap<>();
        p.put("format-version", "2");
        p.put("write.parquet.compression-codec", "zstd");
        // Each commit writes a new metadata.json; without this they pile up forever.
        p.put("write.metadata.delete-after-commit.enabled", "true");
        p.put("write.metadata.previous-versions-max", "100");
        p.put("commit.manifest-merge.enabled", "true");
        return p;
    }

    public static void createIfMissing(CatalogLoader loader) {
        Catalog catalog = loader.loadCatalog();
        for (String ns : List.of("bronze", "silver", "gold")) {
            if (catalog instanceof SupportsNamespaces s && !s.namespaceExists(Namespace.of(ns))) {
                try {
                    s.createNamespace(Namespace.of(ns));
                } catch (org.apache.iceberg.exceptions.AlreadyExistsException ignored) {
                    // another job created it first
                }
            }
        }
        create(catalog, TRIP_EVENTS, TRIP_EVENTS_SCHEMA,
                PartitionSpec.builderFor(TRIP_EVENTS_SCHEMA).day("feed_ts").identity("feed_id").build());
        create(catalog, ARRIVALS, ARRIVALS_SCHEMA,
                PartitionSpec.builderFor(ARRIVALS_SCHEMA).day("arrival_ts").build());
        create(catalog, HEADWAYS, HEADWAYS_SCHEMA,
                PartitionSpec.builderFor(HEADWAYS_SCHEMA).day("arrival_ts").build());
        create(catalog, INCIDENT_EVENTS, INCIDENT_EVENTS_SCHEMA,
                PartitionSpec.builderFor(INCIDENT_EVENTS_SCHEMA).day("event_ts").build());
    }

    private static void create(Catalog catalog, TableIdentifier id, Schema schema, PartitionSpec spec) {
        if (!catalog.tableExists(id)) {
            try {
                catalog.createTable(id, schema, spec, properties());
            } catch (org.apache.iceberg.exceptions.AlreadyExistsException ignored) {
                // created concurrently
            }
        }
    }

    // ---------- converters ----------

    private static StringData s(String v) {
        return v == null ? null : StringData.fromString(v);
    }

    private static TimestampData ts(long ms) {
        return TimestampData.fromEpochMillis(ms);
    }

    private static TimestampData ts(Instant i) {
        return i == null ? null : TimestampData.fromInstant(i);
    }

    private static TimestampData ts(Long ms) {
        return ms == null ? null : TimestampData.fromEpochMillis(ms);
    }

    public static RowData tripEvent(TripEvent e) {
        List<StopPrediction> stops = e.getStops();
        Object[] arr = new Object[stops.size()];
        for (int i = 0; i < arr.length; i++) {
            StopPrediction p = stops.get(i);
            arr[i] = GenericRowData.of(s(p.getStopId()), ts(p.getArrivalTs()), ts(p.getDepartureTs()));
        }
        return GenericRowData.of(
                s(e.getEventType().name()), s(e.getFeedId()), ts(e.getFeedTs()), ts(e.getFetchedAt()),
                s(e.getTripKey()), s(e.getTripId()), s(e.getStartDate()), s(e.getRouteId()), s(e.getDirection()),
                s(e.getVehicleStatus()), s(e.getVehicleStopId()), ts(e.getVehicleTs()), new GenericArrayData(arr));
    }

    public static RowData arrival(Arrival a) {
        return GenericRowData.of(s(a.tripKey), s(a.feedId), s(a.routeId), s(a.direction), s(a.stopId),
                ts(a.arrivalTs), ts(a.observedAt), ts(a.detectedAt));
    }

    public static RowData headway(Headway h) {
        return GenericRowData.of(s(h.routeId), s(h.direction), s(h.stopId), s(h.tripKey), s(h.prevTripKey),
                ts(h.arrivalTs), ts(h.prevArrivalTs), h.headwaySeconds, h.scheduledHeadwaySeconds,
                s(h.status), ts(h.detectedAt));
    }

    public static RowData incident(Incident i) {
        return GenericRowData.of(s(i.incidentId), s(i.eventType), s(i.routeId), s(i.direction), s(i.stopId),
                ts(i.prevArrivalTs), s(i.prevTripKey), ts(i.arrivalTs), s(i.tripKey), ts(i.eventTs),
                i.headwaySeconds, i.scheduledHeadwaySeconds, i.thresholdSeconds, ts(i.detectedAt));
    }
}
