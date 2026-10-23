package io.subwaypulse;

import io.subwaypulse.avro.TripEvent;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Reads Avro records written in the Schema Registry wire format: one magic byte (0),
 * a 4-byte schema id, then the Avro body.
 *
 * <p>The writer schema is fetched from the registry by id once and cached, and records
 * are decoded into the reader schema compiled into this job. That's what makes it
 * safe for the collector to add an optional field: old jobs keep reading new data
 * (the registry is set to BACKWARD compatibility).
 *
 * <p>Flink's own Confluent format pulls the Confluent client from Confluent's Maven
 * repository. This class needs only the registry's REST endpoint.
 */
public class RegistryAvroDeserializer implements DeserializationSchema<TripEvent> {
    private static final long serialVersionUID = 1L;

    private final String registryUrl;
    private transient Map<Integer, SpecificDatumReader<TripEvent>> readers;
    private transient HttpClient http;
    private transient BinaryDecoder decoder;

    public RegistryAvroDeserializer(String registryUrl) {
        this.registryUrl = registryUrl.replaceAll("/+$", "");
    }

    @Override
    public TripEvent deserialize(byte[] message) throws IOException {
        if (message == null || message.length < 5) {
            throw new IOException("message too short for the registry wire format");
        }
        ByteBuffer buf = ByteBuffer.wrap(message);
        if (buf.get() != 0) {
            throw new IOException("unknown magic byte");
        }
        int schemaId = buf.getInt();
        decoder = DecoderFactory.get().binaryDecoder(message, 5, message.length - 5, decoder);
        return reader(schemaId).read(null, decoder);
    }

    private SpecificDatumReader<TripEvent> reader(int id) throws IOException {
        if (readers == null) {
            readers = new HashMap<>();
        }
        SpecificDatumReader<TripEvent> r = readers.get(id);
        if (r == null) {
            Schema writer = new Schema.Parser().parse(fetchSchema(id));
            r = new SpecificDatumReader<>(writer, TripEvent.getClassSchema());
            readers.put(id, r);
        }
        return r;
    }

    String fetchSchema(int id) throws IOException {
        if (http == null) {
            http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        }
        HttpRequest req = HttpRequest.newBuilder(URI.create(registryUrl + "/schemas/ids/" + id + "/schema"))
                .timeout(Duration.ofSeconds(10)).GET().build();
        try {
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new IOException("schema " + id + ": HTTP " + resp.statusCode());
            }
            return resp.body();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    @Override
    public boolean isEndOfStream(TripEvent next) {
        return false;
    }

    @Override
    public TypeInformation<TripEvent> getProducedType() {
        return TypeInformation.of(TripEvent.class);
    }
}
