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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import com.softwareag.ada.reptor.binmessage.MetadataField;
import com.softwareag.ada.reptor.binmessage.MetadataField.Type;
import com.softwareag.ada.reptor.util.MetadataHandler;
import com.softwareag.adabas.targetadapter.sdk.AbstractTarget;
import com.softwareag.adabas.targetadapter.sdk.AdabasObject;
import com.softwareag.adabas.targetadapter.sdk.AdabasObjectData;
import com.softwareag.adabas.targetadapter.sdk.IUserTarget;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * 
 * @author Matthias Gerth, Software AG, Darmstadt, Germany
 *
 */
public class AmazonS3 extends AbstractTarget {

	private static final String BUCKET = "bucket";
	private static final String DIRECTORY = "directory";

	private HashMap<String, String> _schemaMap = new HashMap<String, String>();

	private String _directory;
	private String _bucket;
	private HashMap<String, ArrayList<AdabasObject>> _populateMap = new HashMap<>();
	private S3Client _client = null;
	private boolean _bucketExist = false;
	// private Region _region = Region.EU_CENTRAL_1;

	@Override
	public IUserTarget setParameter(String targetName, HashMap<String, String> parameter) throws Exception {
		if (parameter.containsKey(BUCKET)) {
			_bucket = parameter.get(BUCKET);
		} else {
			throw new Exception("Directory not set.");
		}
		if (parameter.containsKey(DIRECTORY)) {
			_directory = parameter.get(DIRECTORY);
		} else {
			throw new Exception("Directory not set.");
		}
		return this;
	}

	@Override
	public boolean commit(String transactionId) throws Exception {
		if (!_populateMap.isEmpty()) {
			for (Entry<String, ArrayList<AdabasObject>> entry : _populateMap.entrySet()) {
				Schema schema = new Schema.Parser().parse(getSchema(entry.getKey()));
				String path = "adabas/" + entry.getKey().toLowerCase() + "/";
				String objectKey = "p" + System.currentTimeMillis() + ".parquet";
				Path file = FileSystems.getDefault().getPath(_directory + "/" + objectKey);

				Configuration conf = new Configuration();
				try (ParquetWriter<GenericRecord> parquetWriter = AvroParquetWriter
						.<GenericRecord>builder(TargetAdapterOutputFile.nioPathToOutputFile(file)).withSchema(schema)
						.withConf(conf).withCompressionCodec(CompressionCodecName.SNAPPY).build()) {
					for (AdabasObject ao : entry.getValue()) {
						parquetWriter.write(getGenericRecord(ao, schema, entry.getKey()));
					}
				}

				putS3Object(_bucket, path + objectKey, file.toString());
			}
			_populateMap.clear();
		}
		return true;
	}

	@Override
	public void create(AdabasObjectData data) throws Exception {
		printConsole("Create", data);
	}

	@Override
	public void delete(AdabasObjectData data) throws Exception {
		printConsole("Delete", data);
	}

	@Override
	public void insert(AdabasObjectData data) throws Exception {
		printConsole("Insert", data);
	}

	@Override
	public void populate(AdabasObjectData data) throws Exception {
		printConsole("Populate", data);
		putDataInMap(data, _populateMap);
	}

	@Override
	public void update(AdabasObjectData data) throws Exception {
		printConsole("Update", data);
	}

	@Override
	public Object[][] getMetadata() {
		return new Object[][] { { BUCKET, "Bucket-Name", ParameterType.String },
				{ DIRECTORY, "Directory", ParameterType.Directory } };
	}

	@Override
	public boolean isServiceOkay() {
		try {
			_bucketExist = false;
			ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
			ListBucketsResponse listBucketsResponse = getClient().listBuckets(listBucketsRequest);
			listBucketsResponse.buckets().stream().forEach(x -> {
				if (x.name().equals(_bucket)) {
					_bucketExist = true;
				}
			});
			if (!_bucketExist) {
				CreateBucketRequest createBucketRequest = CreateBucketRequest.builder().bucket(_bucket).build();
				getClient().createBucket(createBucketRequest);
			}
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	private S3Client getClient() {
		if (_client == null) {
			_client = S3Client.builder().build();
		}
		return _client;
	}

	private void putDataInMap(AdabasObjectData data, HashMap<String, ArrayList<AdabasObject>> map) throws Exception {
		if (!map.containsKey(data.getFileName())) {
			map.put(data.getFileName(), new ArrayList<AdabasObject>());
		}
		ArrayList<AdabasObject> list = map.get(data.getFileName());
		list.add(data.getAdabasObject());
	}

	private String getFields(String table) throws Exception {
		StringBuilder sb = new StringBuilder("[");
		addField(sb, "ISN", "long", false);
		for (Entry<String, MetadataField> entry : MetadataHandler.getHandler().getMetadataMap(table).entrySet()) {
			// System.out.println(entry.getKey() + " > " + entry.getValue());
			MetadataField field = entry.getValue();
			String type = null;
			if (field.getType() == Type.BASE) {
				switch (field.getFormat()) {
					case STRING:
						type = "string";
						break;
					case DECIMAL:
					case INTEGER:
						type = "int";
						break;
					case FLOAT:
						type = "float";
						break;
					case BINARY:
						type = "bytes";
						break;
					default:
						break;
				}
				if (type != null) {
					addField(sb, field.getName(), type);
				}
			}
		}
		sb.append("]");
		return sb.toString();
	}

	private String getSchema(String table) throws Exception {
		if (!_schemaMap.containsKey(table)) {
			StringBuilder sb = new StringBuilder("{");
			addPair(sb, "type", "record", false);
			addPair(sb, "name", table);
			addPair(sb, "namespace", "com.softwareag.adabas");
			sb.append(",").append("\"fields\":").append(getFields(table));
			sb.append("}");
			_schemaMap.put(table, sb.toString());
		}
		return _schemaMap.get(table);
	}

	private void addField(StringBuilder sb, String name, String type) {
		addField(sb, name, type, true);
	}

	private void addField(StringBuilder sb, String name, String type, boolean comma) {
		if (comma) {
			sb.append(",");
		}
		sb.append('{');
		addPair(sb, "name", name, false);
		addPair(sb, "type", type);
		sb.append('}');
	}

	private void addPair(StringBuilder sb, String key, String value) {
		addPair(sb, key, value, true);
	}

	private void addPair(StringBuilder sb, String key, String value, boolean comma) {
		if (comma) {
			sb.append(",");
		}
		sb.append("\"").append(key).append("\":\"").append(value).append("\"");
	}

	private GenericRecord getGenericRecord(AdabasObject ao, Schema schema, String table) throws Exception {
		GenericRecord record = new GenericData.Record(schema);
		record.put("ISN", ao.evaluateValue("ISN"));
		for (Entry<String, MetadataField> entry : MetadataHandler.getHandler().getMetadataMap(table).entrySet()) {
			MetadataField field = entry.getValue();
			if (field.getType() == Type.BASE) {
				record.put(field.getName(), getObject(field, ao));
			}
		}
		return record;
	}

	private Object getObject(MetadataField field, AdabasObject ao) {
		if (ao.getKeys().contains(field.getName())) {
			return ao.evaluateValue(field.getName());
		} else {
			switch (field.getFormat()) {
				case DECIMAL:
				case INTEGER:
					return 0;
				case FLOAT:
					return 0.0;
				case BINARY:
					return 0x00;
				default:
					break;
			}

		}
		return "";
	}

	private String putS3Object(String bucketName, String objectKey, String objectPath) {

		try {
			// Put a file into the bucket
			PutObjectResponse response = getClient().putObject(
					PutObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
					RequestBody.fromBytes(getObjectFile(objectPath)));

			return response.eTag();

		} catch (S3Exception | FileNotFoundException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
		return "";
	}

	private byte[] getObjectFile(String path) throws FileNotFoundException {

		byte[] bFile = readBytesFromFile(path);
		return bFile;
	}

	private byte[] readBytesFromFile(String filePath) {

		FileInputStream fileInputStream = null;
		byte[] bytesArray = null;

		try {
			File file = new File(filePath);
			bytesArray = new byte[(int) file.length()];

			// read file into bytes[]
			fileInputStream = new FileInputStream(file);
			fileInputStream.read(bytesArray);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fileInputStream != null) {
				try {
					fileInputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return bytesArray;
	}

	private void printConsole(String operation, AdabasObjectData data) throws Exception {
		// System.out.println(operation + " - Table: " + data.getTableName() + ", Key: "
		// + data.getKey() + ", Data:\n"
		// + data.getAdabasObject());
	}

}