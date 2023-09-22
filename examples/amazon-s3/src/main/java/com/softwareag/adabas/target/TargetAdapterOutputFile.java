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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

public class TargetAdapterOutputFile {

	private static final int IO_BUF_SIZE = 16 * 1024;

	public static OutputFile nioPathToOutputFile(Path file) {
		assert file != null;
		return new OutputFile() {
			@Override
			public PositionOutputStream create(long blockSizeHint) throws IOException {
				return makePositionOutputStream(file, IO_BUF_SIZE, false);
			}

			@Override
			public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
				return makePositionOutputStream(file, IO_BUF_SIZE, true);
			}

			@Override
			public boolean supportsBlockSize() {
				return false;
			}

			@Override
			public long defaultBlockSize() {
				return 0;
			}
		};
	}

	private static PositionOutputStream makePositionOutputStream(Path file, int ioBufSize, boolean trunc)
			throws IOException {
		final OutputStream output = new BufferedOutputStream(
				Files.newOutputStream(file, CREATE, trunc ? TRUNCATE_EXISTING : APPEND), ioBufSize);

		return new PositionOutputStream() {
			private long position = 0;

			@Override
			public void write(int b) throws IOException {
				output.write(b);
				position++;
			}

			@Override
			public void write(byte[] b) throws IOException {
				output.write(b);
				position += b.length;
			}

			@Override
			public void write(byte[] b, int off, int len) throws IOException {
				output.write(b, off, len);
				position += len;
			}

			@Override
			public void flush() throws IOException {
				output.flush();
			}

			@Override
			public void close() throws IOException {
				output.close();
			}

			@Override
			public long getPos() throws IOException {
				return position;
			}
		};
	}
}
