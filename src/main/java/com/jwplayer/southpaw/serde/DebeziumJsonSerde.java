package com.jwplayer.southpaw.serde;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.jwplayer.southpaw.record.BaseRecord;
import com.jwplayer.southpaw.record.MapRecord;

import io.debezium.serde.json.JsonSerde;

/**
 * Combines key/value parsing.  This is really not desirable, but follows the pattern of the other Serde classes.
 * It would be better to have separate key and value 
 *  - key can be a simple type or BaseRecord, and value can be BaseRecord.
 *  
 * Alternatively this could be implemented using JsonNode and a new {@link BaseRecord} type.
 */
public class DebeziumJsonSerde implements BaseSerde<BaseRecord> {

    private JsonSerde<?> serde;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Class<?> clazz = Map.class;
        if (!isKey) {
            if (configs.get("from.field") == null) {
                HashMap<String, Object> config = new HashMap<>(configs);
                config.put("from.field", "after");
                configs = config;
            }
        } else {
            String type = (String)configs.get("key.type");
            if (type != null) {
                try {
                    clazz = Class.forName(type);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        serde = new JsonSerde<>(clazz);
        serde.configure(configs, isKey);
    }

    @Override
    public void close() {
        serde.close();
    }

    @Override
    public Serializer<BaseRecord> serializer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Deserializer<BaseRecord> deserializer() {
        Deserializer<?> deserializer = serde.deserializer();
        return new Deserializer<BaseRecord>() {

            @Override
            public BaseRecord deserialize(String topic, byte[] data) {
                Object result = deserializer.deserialize(topic, data);
                if (result instanceof Map) {
                    return new MapRecord((Map)result);
                }
                //provide a wrapper for a non-object type
                return new MapRecord(Collections.singletonMap("id", result));
            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                deserializer.configure(configs, isKey);
            }

            @Override
            public void close() {
                deserializer.close();
            }
        };
    }

}
