/*
 * Copyright © 2018-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.format.avro.input;

import com.google.common.base.Strings;
import com.google.common.io.Files;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.plugin.format.avro.AvroToStructuredTransformer;
import io.cdap.plugin.format.input.PathTrackingConfig;
import io.cdap.plugin.format.input.PathTrackingInputFormatProvider;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides and sets up configuration for an AvroInputFormat.
 */
@Plugin(type = ValidatingInputFormat.PLUGIN_TYPE)
@Name(AvroInputFormatProvider.NAME)
@Description(AvroInputFormatProvider.DESC)
public class AvroInputFormatProvider extends PathTrackingInputFormatProvider<AvroInputFormatProvider.Conf> {
  static final String NAME = "avro";
  static final String DESC = "Plugin for reading files in avro format.";
  public static final PluginClass PLUGIN_CLASS =
      new PluginClass(ValidatingInputFormat.PLUGIN_TYPE, NAME, DESC, AvroInputFormatProvider.class.getName(),
          "conf", PathTrackingConfig.FIELDS);

  public AvroInputFormatProvider(AvroInputFormatProvider.Conf conf) {
    super(conf);
  }

  @Override
  public String getInputFormatClassName() {
    return CombineAvroInputFormat.class.getName();
  }

  @Override
  protected void addFormatProperties(Map<String, String> properties) {
    Schema schema = conf.getSchema();
    if (schema != null) {
      properties.put("avro.schema.input.key", schema.toString());
    }
  }

  @Override
  public void validate(FormatContext context) {
    getSchema(context);
    super.validate(context);
  }

  /**
   * Common config for Avro format
   */
  public static class Conf extends PathTrackingConfig {

    @Macro
    @Nullable
    @Description(NAME_SCHEMA)
    public String schema;

    @Nullable
    @Override
    public Schema getSchema() {
      return super.getSchema();
    }
  }

  @Nullable
  @Override
  public Schema getSchema(FormatContext context) {
    if (conf.containsMacro("schema")) {
      return super.getSchema(context);
    }
    if (!Strings.isNullOrEmpty(conf.schema)) {
      return super.getSchema(context);
    }
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
    String filePath = conf.getProperties().getProperties().getOrDefault("path", null);
    if (filePath == null) {
      return super.getSchema(context);
    }
    File file = pickFile(filePath, "avro", context.getFailureCollector());
    try {
      DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
      GenericRecord firstRecord;
      if (!dataFileReader.hasNext()) {
        return null;
      }
      firstRecord = dataFileReader.next();
      return new AvroToStructuredTransformer().convertSchema(firstRecord.getSchema());
    } catch (IOException e) {
      context.getFailureCollector().addFailure("Schema parse error", e.getMessage());
    }
    return super.getSchema(context);
  }

  /**
   * Checks whether provided path is directory or file and returns file based on the following conditions:
   * if provided path directs to file - file from the provided path will be returned
   * if provided path directs to a directory - first file matching the extension will be provided
   * if extension is null first file from the directory will be returned
   *
   * @param path              path from config
   * @param matchingExtension extension to match when searching for file in directory
   * @param collector         {@link FailureCollector}
   * @return {@link File}
   */
  private File pickFile(String path, String matchingExtension, FailureCollector collector) {
    final File filePath = Paths.get(path).toFile();
    if (filePath.isFile()) {
      return filePath;
    }
    // read directory files
    final File[] files = filePath.listFiles();
    if (files == null) {
      collector.addFailure("Path issue", "Cannot read files from provided path");
      return null;
    }
    if (files.length == 0) {
      collector.addFailure("Path issue", "Provided directory is empty");
      return null;
    }
    // find first avro file
    for (File file : files) {
      if (matchingExtension == null) {
        return file;
      }
      if (Files.getFileExtension(file.getName()).equals(matchingExtension)) {
        return file;
      }
    }
    collector.addFailure("Path issue", "Could not find AVRO file found in provided path");
    return null;
  }

}
