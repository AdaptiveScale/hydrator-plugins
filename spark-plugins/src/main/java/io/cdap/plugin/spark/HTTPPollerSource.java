/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.plugin.spark;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.plugin.common.http.HTTPPollConfig;
import io.cdap.plugin.common.http.HTTPRequestor;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Polls a http endpoints and outputs a record for each url response.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("HTTPPoller")
@Description("Fetch data by performing an HTTP request at a regular interval.")
public class HTTPPollerSource extends StreamingSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(HTTPPollerSource.class);
  private final HTTPPollConfig conf;

  public HTTPPollerSource(HTTPPollConfig conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    conf.validate(collector);
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) throws Exception {
    FailureCollector collector = streamingContext.getFailureCollector();
    conf.validate(collector);
    collector.getOrThrowException();

    return streamingContext.getSparkStreamingContext()
      .receiverStream(new Receiver<StructuredRecord>(StorageLevel.MEMORY_ONLY()) {

        @Override
        public StorageLevel storageLevel() {
          return StorageLevel.MEMORY_ONLY();
        }

        @Override
        public void onStart() {
          new Thread() {

            @Override
            public void run() {
              HTTPRequestor httpRequestor = new HTTPRequestor(conf);
              while (!isStopped()) {

                try {

                  store(httpRequestor.get());
                } catch (Exception e) {
                  LOG.error("Error getting content from {}.", conf.getUrl(), e);
                }

                try {
                  TimeUnit.SECONDS.sleep(conf.getInterval());
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }
            }

            @Override
            public void interrupt() {
              super.interrupt();
            }
          }.start();
        }

        @Override
        public void onStop() {

        }
      });
  }
}
