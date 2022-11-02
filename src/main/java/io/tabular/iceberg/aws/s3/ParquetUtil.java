/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.aws.s3;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;

public class ParquetUtil {

  public static void checkForCorruption(File file) {
    if (!file.exists()) {
      return;
    }

    try (ParquetFileReader reader = ParquetFileReader.open(new ParquetInputFile(file))) {
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();

      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        for (ColumnDescriptor colDesc : schema.getColumns()) {
          PageReader pageReader = pages.getPageReader(colDesc);

          DictionaryPage dictPage = pageReader.readDictionaryPage();
          if (dictPage != null) {
            try {
              dictPage.getBytes().toByteArray();
            } catch (IOException x) {
              throw new ParquetDecodingException(x);
            }
          }

          DataPage page;
          while ((page = pageReader.readPage()) != null) {
            page.accept(
                new DataPage.Visitor<Void>() {
                  @Override
                  public Void visit(DataPageV1 dataPageV1) {
                    try {
                      dataPageV1.getBytes().toByteArray();
                    } catch (IOException x) {
                      throw new ParquetDecodingException(x);
                    }
                    return null;
                  }

                  @Override
                  public Void visit(DataPageV2 dataPageV2) {
                    try {
                      dataPageV2.getData().toByteArray();
                    } catch (IOException x) {
                      throw new ParquetDecodingException(x);
                    }
                    return null;
                  }
                });
          }
        }
      }
    } catch (Exception x) {
      throw new RuntimeException("Potential corruption of file: " + file);
    }
  }

  static class ParquetInputFile implements InputFile {

    private final File file;

    public ParquetInputFile(File file) {
      this.file = file;
    }

    @Override
    public long getLength() {
      return file.length();
    }

    @Override
    public SeekableInputStream newStream() {
      try {
        return new ParquetInputStreamAdapter(new FileInputStream(file));
      } catch (IOException x) {
        throw new UncheckedIOException(x);
      }
    }
  }

  static class ParquetInputStreamAdapter extends DelegatingSeekableInputStream {

    private final FileInputStream delegate;

    public ParquetInputStreamAdapter(FileInputStream delegate) {
      super(delegate);
      this.delegate = delegate;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getChannel().position();
    }

    @Override
    public void seek(long newPos) throws IOException {
      delegate.getChannel().position(newPos);
    }
  }
}
