/*
 * Copyright (c) 2021 Software AG, Darmstadt, Germany and/or Software AG USA 
 * Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates
 * and/or their licensors.
 * Use, reproduction, transfer, publication or disclosure is prohibited except
 * as specifically provided for in your License Agreement with Software AG.
 *
 */
package com.softwareag.adabas.target;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.softwareag.adabas.targetadapter.sdk.AdabasObject;
import com.softwareag.adabas.targetadapter.sdk.AdabasObjectData;

public class KafkaTest {
	private static final long ISN = 42l;
	
	private Kafka _kafka;
	private AdabasObjectData _data;
	private AdabasObjectData _date;
	
	@Before
	public void before() throws Exception {
		_kafka = new Kafka();
		HashMap<String, String> parameter = new HashMap<>();
		parameter.put("propertiesFile", "./src/test/resources/kafka.properties");
		_kafka.setParameter("kafkatest", parameter);
	}
	
	@Test
	public void create() throws Exception {
		_data = new AdabasObjectData("0", "EMPL_EMPLOYEES", "SUBS", 47, 11, null, null);
		_data.setAdabasObject(createObject());
		_kafka.create(_data);
		_kafka.commit("0");
	}

	@Test
	public void insert() throws Exception {
		_data = new AdabasObjectData("1", "EMPL_EMPLOYEES", "SUBS", 47, 11, null, null);
		AdabasObject adabasObject = insertObject();
		_data.setAdabasObject(adabasObject);
		_kafka.insert(_data);
		_kafka.commit("1");
	}
	
	@Test
	public void insertDate() throws Exception {
		_date = new AdabasObjectData("2", "DATE", "DATE", 47, 11, null, null);
		AdabasObject adabasObject = AdabasObject.newObject();
		adabasObject.putValue("DATE", new Date());
		System.out.println(adabasObject);
		_date.setAdabasObject(adabasObject);
		_kafka.insert(_date);
		_kafka.commit("2");
	}


	@Test
	public void populate() throws Exception {
		_data = new AdabasObjectData("3", "EMPL_EMPLOYEES", "SUBS", 47, 11, null, null);
		AdabasObject adabasObject = insertObject();
		_data.setAdabasObject(adabasObject);
		_kafka.populate(_data);
		_kafka.commit("3");
	}

	@Test
	public void update() throws Exception {
		_data = new AdabasObjectData("4", "EMPL_EMPLOYEES", "SUBS", 47, 11, null, null);
		AdabasObject adabasObject = AdabasObject.newObject();
		adabasObject.putValue("FIRST_NAME", "first");
		adabasObject.putValue("CITY", "Frankfurt");
		adabasObject.putValue("ISN", ISN);
		_data.setAdabasObject(adabasObject);
		_kafka.update(_data);
		_kafka.commit("4");
	}

	@Test
	public void delete() throws Exception {
		_data = new AdabasObjectData("5", "EMPL_EMPLOYEES", "SUBS", 47, 11, null, null);
		AdabasObject adabasObject = AdabasObject.newObject();
		adabasObject.putValue("ISN", ISN);
		_data.setAdabasObject(adabasObject);
		_kafka.delete(_data);
		_kafka.commit("5");
	}
	
	@After
	public void after() throws Exception {
		_kafka.close();
	}

	/**
	 * @return object for create statement
	 */
	private AdabasObject createObject() {
		AdabasObject create = AdabasObject.newObject();
		create.putValue("AREACODE", "string,6,0,");
		create.putValue("CITY", "string,20,0,ky");
		create.putValue("COUNTRY", "string,3,0,");
		create.putValue("DEPT", "string,6,0,ky");
		create.putValue("FIRST_NAME", "string,20,0,");
		create.putValue("JOBTITLE", "string,25,0,ky");
		create.putValue("LEAVE_DUE", "decimal,2,0,");
		create.putValue("LEAVE_TAKEN", "decimal,2,0,");
		create.putValue("MARSTAT", "string,1,0,");
		create.putValue("MIDDLE_NAME", "string,20,0,");
		create.putValue("NAME", "string,20,0,ky");
		create.putValue("PERSONNEL_ID", "string,8,0,uk");
		create.putValue("PHONE", "string,15,0,");
		create.putValue("POSTCODE", "string,10,0,");
		create.putValue("SEX", "string,1,0,");
		ArrayList<String> mu = new ArrayList<>();
		mu.add("string,20,0,");
		create.putValue("ADDRESS_LINE", mu);
		mu = new ArrayList<>();
		mu.add("string,3,0,");
		create.putValue("LANG", mu);
		ArrayList<AdabasObject> income = new ArrayList<>();
		AdabasObject pe = AdabasObject.newObject();
		pe.putValue("CURRCODE", "string,3,0,");
		pe.putValue("SALARY", "decimal,10,0,");
		ArrayList<String> bonus = new ArrayList<>();
		bonus.add("decimal,10,0,");
		pe.putValue("BONUS", bonus);
		income.add(pe);
		create.putValue("INCOME", income);
		ArrayList<AdabasObject> leave = new ArrayList<>();
		pe = AdabasObject.newObject();
		pe.putValue("LEAVE_END", "decimal,8,0,");
		pe.putValue("LEAVE_START", "decimal,8,0,");
		leave.add(pe);
		return create;
	}
	
	/**
	 * @return object to insert
	 */
	private AdabasObject insertObject() {
		AdabasObject adabasObject = AdabasObject.newObject();
		// fields
		adabasObject.putValue("NAME", "Mustermann");
		adabasObject.putValue("MIDDLE_NAME", "M.");
		adabasObject.putValue("FIRST_NAME", "Maximilian");
		adabasObject.putValue("CITY", "Darmstadt");
		adabasObject.putValue("COUNTRY", "D");
		adabasObject.putValue("ISN", ISN);
		adabasObject.putValue("PERSONNEL_ID", "TEST0002");
		// MU
		ArrayList<String> lang = new ArrayList<>();
		lang.add("GER");
		lang.add("ENG");
		lang.add("ESP");
		adabasObject.putValue("LANG", lang);
		// PE
		ArrayList<AdabasObject> income = new ArrayList<>();
		for (int i = 2; i > 0; i--) {
			AdabasObject pe = AdabasObject.newObject();
			pe.putValue("CURRCODE", "USD");
			pe.putValue("SALARY", (i + 1) * 50000);
			ArrayList<Integer> bonus = new ArrayList<>();
			for (int j = 0; j < i; j++) {
				bonus.add((j + 1) * 1000);
			}
			pe.putValue("BONUS", bonus);
			income.add(pe);
		}
		adabasObject.putValue("INCOME", income);
		return adabasObject;
	}

}
