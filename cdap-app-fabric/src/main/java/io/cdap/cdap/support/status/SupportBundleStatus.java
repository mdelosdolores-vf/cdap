/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.support.status;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;

/**
 * Status when generating Support bundle
 */
public class SupportBundleStatus {
  // UUID of the bundle status object describes
  @SerializedName("bundle-id")
  private String bundleId;
  // status of bundle collection (IN_PROGRESS/FINISHED/FAILED)
  private CollectionState status;
  // Failed bundle describes the failure
  @SerializedName("status-details")
  private String statusDetails;
  // when bundle collection was started
  @SerializedName("start-timestamp")
  private Long startTimestamp;
  // FINISHED/FAILED bundles when bundle collection was completed
  @SerializedName("finish-timestamp")
  private Long finishTimestamp;
  // any parameters passed to start collection
  private SupportBundleConfiguration parameters;
  // Array of top-level tasks for the bundle, see task structure below
  private List<SupportBundleTaskStatus> tasks = new ArrayList<>();

  public CollectionState getStatus() {
    return status;
  }

  public void setStatus(CollectionState status) {
    this.status = status;
  }

  public String getStatusDetails() {
    return statusDetails;
  }

  public void setStatusDetails(String statusDetails) {
    this.statusDetails = statusDetails;
  }

  public Long getStartTimestamp() {
    return startTimestamp;
  }

  public void setStartTimestamp(Long startTimestamp) {
    this.startTimestamp = startTimestamp;
  }

  public Long getFinishTimestamp() {
    return finishTimestamp;
  }

  public void setFinishTimestamp(Long finishTimestamp) {
    this.finishTimestamp = finishTimestamp;
  }

  public SupportBundleConfiguration getParameters() {
    return parameters;
  }

  public void setParameters(SupportBundleConfiguration parameters) {
    this.parameters = parameters;
  }

  public List<SupportBundleTaskStatus> getTasks() {
    return tasks;
  }

  public void setTasks(List<SupportBundleTaskStatus> tasks) {
    this.tasks = tasks;
  }

  public String getBundleId() {
    return bundleId;
  }

  public void setBundleId(String bundleId) {
    this.bundleId = bundleId;
  }
}