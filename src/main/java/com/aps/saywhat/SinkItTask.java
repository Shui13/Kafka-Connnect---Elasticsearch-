package com.aps.saywhat;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class SinkItTask extends SinkTask {
	private String indexPrefix;
	private final String TYPE_NAME = "log";
	private TransportClient client;
	DateFormat dateFormat;

	public String getDate() {
		return dateFormat.format(new Date());
	}

	@Override
	public void start(Map<String, String> props) {
		final String esHost = props.get(SInkItConnector.ES_HOST);
		indexPrefix = props.get(SInkItConnector.INDEX_PREFIX);
		try {
			Settings settings = Settings.builder().put("cluster.name", "magicMike").build();
			client = new PreBuiltTransportClient(settings)
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(esHost), 9300));
			dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		} catch (UnknownHostException ex) {
			throw new ConnectException("Couldn't connect to es host", ex);
		}
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		for (SinkRecord record : records) {
			ObjectMapper objectMapper = new ObjectMapper();
			try {
				if (null != record.key()) {
					IndexResponse indexResponse = client
							.prepareIndex(indexPrefix + ((String) record.key()).toLowerCase() + "-" + getDate(),
									TYPE_NAME)
							.setSource(objectMapper.writeValueAsString(record.value())).get();
				}
			} catch (JsonProcessingException e) {
				System.out.println("Exception while converting hashmap into json string");
				e.printStackTrace();
			}
		}
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
	}

	@Override
	public void stop() {
		client.close();
	}

	@Override
	public String version() {
		return "v1";
	}
}
