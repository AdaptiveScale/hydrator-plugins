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

package io.cdap.plugin.format.parquet.input;

import com.google.common.io.Files;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.plugin.format.input.PathTrackingConfig;
import io.cdap.plugin.format.input.PathTrackingInputFormatProvider;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Strings;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides and sets up configuration for an parquet input format.
 */
@Plugin(type = ValidatingInputFormat.PLUGIN_TYPE)
@Name(ParquetInputFormatProvider.NAME)
@Description(ParquetInputFormatProvider.DESC)
public class ParquetInputFormatProvider extends
    PathTrackingInputFormatProvider<ParquetInputFormatProvider.Conf> {

  static final String NAME = "parquet";
  static final String DESC = "Plugin for reading files in text format.";
  public static final PluginClass PLUGIN_CLASS =
      new PluginClass(ValidatingInputFormat.PLUGIN_TYPE, NAME, DESC,
          ParquetInputFormatProvider.class.getName(),
          "conf", PathTrackingConfig.FIELDS);

  public ParquetInputFormatProvider(ParquetInputFormatProvider.Conf conf) {
    super(conf);
  }

  @Override
  public String getInputFormatClassName() {
    return CombineParquetInputFormat.class.getName();
  }

  @Override
  protected void addFormatProperties(Map<String, String> properties) {
    Schema schema = conf.getSchema();
    if (schema != null) {
      properties.put("parquet.avro.read.schema", schema.toString());
    }
  }

  @Nullable
  @Override
  public Schema getSchema(FormatContext context) {
    if (conf.containsMacro(PathTrackingConfig.NAME_SCHEMA) || !Strings.isNullOrEmpty(conf.schema)) {
      return super.getSchema(context);
    }
    String filePath = conf.getProperties().getProperties().getOrDefault("path", null);

    try {
      File file = pickFile(filePath, "parquet");
      final ParquetReader reader = AvroParquetReader.<GenericData.Record>builder(
          new Path(file.getAbsolutePath()))
          .build();
      GenericData.Record record = (GenericData.Record) reader.read();
      return Schema.parseJson(record.getSchema().toString());
    } catch (IOException e) {
      context.getFailureCollector().addFailure("Schema error", e.getMessage());
    }
    return super.getSchema(context);
  }

  /**
   * Common config for Parquet format
   */
  public static class Conf extends PathTrackingConfig {

    @Macro
    @Nullable
    @Description(NAME_SCHEMA)
    public String schema;
  }

  /**
   * Checks whether provided path is directory or file and returns file based on the following
   * conditions: if provided path directs to file - file from the provided path will be returned if
   * provided path directs to a directory - first file matching the extension will be provided if
   * extension is null first file from the directory will be returned
   *
   * @param path              path from config
   * @param matchingExtension extension to match when searching for file in directory
   * @return {@link File}
   */
  private File pickFile(String path, String matchingExtension) {
    final File filePath = Paths.get(path).toFile();
    if (filePath.isFile()) {
      return filePath;
    }
    // read directory files
    final File[] files = filePath.listFiles();
    if (files == null) {
      throw new IllegalArgumentException("Cannot read files from provided path");
    }
    if (files.length == 0) {
      throw new IllegalArgumentException("Provided directory is empty");
    }
    // find first file
    for (File file : files) {
      if (matchingExtension == null) {
        return file;
      }
      if (Files.getFileExtension(file.getName()).equals(matchingExtension)) {
        return file;
      }
    }
    throw new IllegalArgumentException("Could not find a file in provided path");
  }
}
