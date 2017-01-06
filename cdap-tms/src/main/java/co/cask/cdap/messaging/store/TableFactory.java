/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.messaging.store;

import java.io.IOException;

/**
 * A factory to create instances of {@link MessageTable}, {@link PayloadTable} and {@link MetadataTable}.
 */
public interface TableFactory {

  MetadataTable createMetadataTable(String tableName) throws IOException;

  MessageTable createMessageTable(String tableName) throws IOException;

  PayloadTable createPayloadTable(String tableName) throws IOException;

  void upgradeTables(String messageTableName, String payloadTableName) throws IOException;
}
