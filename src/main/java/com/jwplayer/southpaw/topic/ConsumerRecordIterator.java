package com.jwplayer.southpaw.topic;

import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerRecordIterator<K, V> extends Iterator<ConsumerRecord<K, V>>{

    ConsumerRecord<byte[], byte[]> peekRawConsumerRecord();

    V peekValue();

}