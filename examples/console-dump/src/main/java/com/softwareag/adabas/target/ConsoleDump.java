/*
 * Copyright 2021 Software AG
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.softwareag.adabas.targetadapter.sdk.AbstractTarget;
import com.softwareag.adabas.targetadapter.sdk.AdabasObjectData;

public class ConsoleDump extends AbstractTarget {

	private static final Logger logger = LogManager.getLogger(ConsoleDump.class);

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
	}

	@Override
	public void update(AdabasObjectData data) throws Exception {
		printConsole("Update", data, true);
	}

	@Override
	public boolean commit(String transactionId) throws Exception {
		logger.info("Commit " + transactionId);
		return true;
	}

	@Override
	public void command(String operation, int dbid, int fnr, String state, String subscription, String fileName,
			String value) throws Exception {
		logger.info("Operation: {}, DBID: {}, FNR: {}, state: {}, subscription: {}, fileName: {}, value: {}", operation,
				dbid, fnr, state, subscription, fileName, value);
	}

	private void printConsole(String operation, AdabasObjectData data) throws Exception {
		printConsole(operation, data, false);
	}

	private void printConsole(String operation, AdabasObjectData data, boolean beforeObject) throws Exception {
		if (beforeObject) {
			logger.info("{} - {}:\n{}Before Object\n{}", operation, data, data.getAdabasObject(),
					data.getBeforeObject());
		} else {
			logger.info("{} - {}:\n{}", operation, data, data.getAdabasObject());
		}
	}

}
