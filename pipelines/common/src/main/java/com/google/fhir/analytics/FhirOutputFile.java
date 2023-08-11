/*
 * Copyright 2020-2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.fhir.analytics;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * This is an implementation of the OutputFile. This class is used by the AvroParquetWriter for
 * writing the FHIR resources into parquet files. This class instead of directly writing into the
 * files, it writes into the OutputStream which is passed during class instantiation.
 */
public class FhirOutputFile implements OutputFile {
  private final OutputStream outputStream;

  FhirOutputFile(OutputStream outputStream) {
    this.outputStream = outputStream;
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) {
    return new FhirOutputStream(outputStream);
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) {
    return new FhirOutputStream(outputStream);
  }

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() {
    return 0;
  }

  private static class FhirOutputStream extends PositionOutputStream {
    private long position = 0;
    private final OutputStream outputStream;

    private FhirOutputStream(OutputStream outputStream) {
      this.outputStream = outputStream;
    }

    @Override
    public long getPos() {
      return position;
    }

    @Override
    public void write(int b) throws IOException {
      position++;
      outputStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      outputStream.write(b, off, len);
      position += len;
    }

    @Override
    public void flush() throws IOException {
      outputStream.flush();
    }

    @Override
    public void close() throws IOException {
      outputStream.close();
    }
  }
}
