/*
 * Copyright (c) 2021-2023 Software AG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.softwareag.adabas.target;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.softwareag.adabas.targetadapter.sdk.AbstractTarget;
import com.softwareag.adabas.targetadapter.sdk.AdabasObject;
import com.softwareag.adabas.targetadapter.sdk.AdabasObjectData;
import com.softwareag.adabas.targetadapter.sdk.IUserTarget;

public class Kafka extends AbstractTarget {

	private static final Logger logger = LogManager.getLogger();

	private final String PROPERTIES_FILE = "propertiesFile";
	private Producer<String, String> _producer;

	private ArrayList<ProducerRecord<String, String>> _list = null;

	public Kafka() {
	}

	@Override
	public IUserTarget setParameter(String targetName, HashMap<String, String> parameter) throws Exception {
		String fileName = null;
		if (parameter.containsKey(PROPERTIES_FILE)) {
			fileName = parameter.get(PROPERTIES_FILE);
		} else {
			throw new Exception("Properties file not set.");
		}

		Properties props = new Properties();
		try {
			File file = new File(fileName);
			logger.info("Reading Kafka properties from " + file.getAbsolutePath());
			props.load(new FileInputStream(file));
		} catch (Exception e) {
			logger.info(e.getLocalizedMessage());
		}
		if (!props.containsKey("bootstrap.servers"))
			throw new Exception("No bootstrap.servers defined.");
		if (!props.containsKey("acks"))
			props.put("acks", "all");
		if (!props.containsKey("key.serializer"))
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		if (!props.containsKey("value.serializer"))
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		logger.info("Used properties for the KafkaProducer:");
		for (Entry<Object, Object> prop : props.entrySet()) {
			logger.info(prop.getKey() + "=" + prop.getValue());
		}
		_producer = new KafkaProducer<String, String>(props);
		return this;
	}

	@Override
	public boolean commit(String transactionId) throws Exception {
		if (_list != null && !_list.isEmpty()) {
			for (ProducerRecord<String, String> record : _list) {
				_producer.send(record, (recordMetadata, error) -> {
					if (error != null) {
						logger.error(error.getLocalizedMessage());
					} else {
						logger.debug("Record successfully appended to topic " + recordMetadata.topic());
					}
				});
			}
			_list = null;
		}
		return true;
	}

	@Override
	public void populate(AdabasObjectData data) throws Exception {
		getList().add(createMessageString(data, "Populate"));
	}

	@Override
	public void create(AdabasObjectData data) throws Exception {
		getList().add(createMessageString(data, "Create"));
	}

	@Override
	public void delete(AdabasObjectData data) throws Exception {
		getList().add(createMessageString(data, "Delete"));
	}

	@Override
	public void insert(AdabasObjectData data) throws Exception {
		getList().add(createMessageString(data, "Insert"));
	}

	@Override
	public void update(AdabasObjectData data) throws Exception {
		getList().add(createMessageString(data, "Update"));
	}

	@Override
	public Object[][] getMetadata() {
		return new Object[][] { { PROPERTIES_FILE, "Properties File", ParameterType.File } };
	}

	@Override
	public void close() throws Exception {
		_producer.close();
	}

	private ProducerRecord<String, String> createMessageString(AdabasObjectData data, String command) throws Exception {
		return createMessageString(data.getAdabasObject(), data.getFileName(), command);
	}

	private static final Gson gson = new GsonBuilder()
			.setPrettyPrinting()
			.create();

	private ProducerRecord<String, String> createMessageString(AdabasObject ao, String table, String command)
			throws Exception {
		Object isn = ao.evaluateValue("ISN");
		String key = isn == null ? null : isn.toString();
		JsonObject message = new JsonObject();
		message.addProperty("method", command);
		message.add("data", createJSON(ao));
		return new ProducerRecord<String, String>(table, key, gson.toJson(message));
	}

	private JsonObject createJSON(AdabasObject ao) {
		JsonObject json = new JsonObject();
		for (String key : ao.getKeyList()) {
			Object object = ao.evaluateValue(key);
			if (object instanceof AdabasObject) {
				json.add(key, createJSON((AdabasObject) object));
			} else {
				if (object instanceof List<?>) {
					ArrayList<Object> list = new ArrayList<>();
					for (Object obj : (List<?>) object) {
						if (obj instanceof AdabasObject) {
							list.add(createJSON((AdabasObject) obj));
						} else {
							list.add(obj);
						}
					}

					json.add(key, new Gson().toJsonTree(list).getAsJsonArray());
				} else {
					if (object instanceof Date) {
						json.addProperty(key, object.toString());
					} else if (object instanceof Number) {
						Number num = (Number) object;
						json.addProperty(key, num);
					} else {
						json.addProperty(key, object.toString());
					}
				}
			}
		}
		return json;
	}

	/**
	 * @return the List of commands
	 */
	private ArrayList<ProducerRecord<String, String>> getList() {
		if (_list == null) {
			_list = new ArrayList<ProducerRecord<String, String>>();
		}
		return _list;
	}
}
