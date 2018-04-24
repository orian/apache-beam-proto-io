/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.pawelsz.apache.beam.io.protoio;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.io.BaseEncoding;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.DynamicAvroDestinations;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Map;

/** Always returns a constant {@link FilenamePolicy}, {@link Schema}, metadata, and codec. */
class ConstantProtoDestination<UserT, OutputT>
    extends DynamicProtoDestinations<UserT, Void, OutputT> {

  // This should be a multiple of 4 to not get a partial encoded byte.
  private static final int METADATA_BYTES_MAX_LENGTH = 40;
  private final FilenamePolicy filenamePolicy;
  private final Map<String, Object> metadata;
  private final SerializableFunction<UserT, OutputT> formatFunction;

  private class Metadata implements HasDisplayData {
    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      for (Map.Entry<String, Object> entry : metadata.entrySet()) {
        DisplayData.Type type = DisplayData.inferType(entry.getValue());
        if (type != null) {
          builder.add(DisplayData.item(entry.getKey(), type, entry.getValue()));
        } else {
          String base64 = BaseEncoding.base64().encode((byte[]) entry.getValue());
          String repr =
              base64.length() <= METADATA_BYTES_MAX_LENGTH
                  ? base64
                  : base64.substring(0, METADATA_BYTES_MAX_LENGTH) + "...";
          builder.add(DisplayData.item(entry.getKey(), repr));
        }
      }
    }
  }

  public ConstantProtoDestination(
      FilenamePolicy filenamePolicy,
      Map<String, Object> metadata,
      SerializableFunction<UserT, OutputT> formatFunction) {
    this.filenamePolicy = filenamePolicy;
    this.metadata = metadata;
    this.formatFunction = formatFunction;
  }

  @Override
  public OutputT formatRecord(UserT record) {
    return formatFunction.apply(record);
  }

  @Override
  @Nullable
  public Void getDestination(UserT element) {
    return (Void) null;
  }

  @Override
  @Nullable
  public Void getDefaultDestination() {
    return (Void) null;
  }

  @Override
  public FilenamePolicy getFilenamePolicy(Void destination) {
    return filenamePolicy;
  }

  @Override
  public Map<String, Object> getMetadata(Void destination) {
    return metadata;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    filenamePolicy.populateDisplayData(builder);
    builder.include("Metadata", new Metadata());
  }
}
