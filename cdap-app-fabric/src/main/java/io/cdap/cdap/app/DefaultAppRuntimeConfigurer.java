/*
 * Copyright © 2022 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.app;

import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.app.RuntimeConfigurer;
import io.cdap.cdap.app.services.AbstractServiceDiscoverer;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Default app configurer for runtime deployment
 */
public class DefaultAppRuntimeConfigurer extends AbstractServiceDiscoverer implements RuntimeConfigurer {
  private final RemoteClientFactory remoteClientFactory;
  private final Map<String, String> userArguments;
  private final ApplicationSpecification deployedAppSpec;

  public DefaultAppRuntimeConfigurer(String namespace,
                                     RemoteClientFactory remoteClientFactory, Map<String, String> userArguments,
                                     @Nullable ApplicationSpecification deployedAppSpec) {
    super(namespace);
    this.remoteClientFactory = remoteClientFactory;
    this.userArguments = new HashMap<>(userArguments);
    this.deployedAppSpec = deployedAppSpec;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return userArguments;
  }

  @Override
  public ApplicationSpecification getDeployedApplicationSpec() {
    return deployedAppSpec;
  }

  @Override
  @Nullable
  protected RemoteClientFactory getRemoteClientFactory() {
    return remoteClientFactory;
  }
}
