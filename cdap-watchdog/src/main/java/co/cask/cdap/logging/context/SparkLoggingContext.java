/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.logging.context;

import co.cask.cdap.common.logging.ApplicationLoggingContext;

/**
 * Spark logging context
 */
public class SparkLoggingContext extends ApplicationLoggingContext {

  public static final String TAG_SPARK_JOB_ID = ".sparkId";

  /**
   * Constructs the SparkLoggingContext.
   *
   * @param namespaceId namespace id
   * @param applicationId application id
   * @param sparkId spark job id
   */
  public SparkLoggingContext(final String namespaceId, final String applicationId, final String sparkId) {
    super(namespaceId, applicationId);
    setSystemTag(TAG_SPARK_JOB_ID, sparkId);
  }

  @Override
  public String getLogPartition() {
    return String.format("%s:%s", super.getLogPartition(), getSystemTag(TAG_SPARK_JOB_ID));
  }

  @Override
  public String getLogPathFragment() {
    return String.format("%s/spark-%s", super.getLogPathFragment(), getSystemTag(TAG_SPARK_JOB_ID));
  }
}
