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

package io.cdap.cdap.internal.app.runtime.distributed;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.DefaultNamespaceStore;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionModules;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class DistributedProgramRunnerTest {

  private static final String NAMESPACE = "namespace";
  private static final Map<String, String> CONFIGS = Collections.singletonMap("property_1", "value_1");

  private static DefaultNamespaceStore nsStore;
  private static DistributedProgramRunner distributedProgramRunner;
  private static Injector injector;
  private static TransactionManager txManager;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    injector = Guice.createInjector(
      new ConfigModule(cConf),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new TransactionModules().getInMemoryModules(),
      new StorageModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
        }
      });
    nsStore = new DefaultNamespaceStore(injector.getInstance(TransactionRunner.class));
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    // Define all StructuredTable before starting any services that need StructuredTable
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class));

    distributedProgramRunner = new DistributedServiceProgramRunner(cConf, null, null, ClusterMode.ON_PREMISE, null);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    txManager.stopAndWait();
  }

  @Test
  public void testGetNamespaceConfigs() {
    NamespaceMeta meta = new NamespaceMeta.Builder().setName(NAMESPACE).setConfig(CONFIGS).build();
    nsStore.create(meta);

    Map<String, String> foundNamespaceConfigs = distributedProgramRunner.getNamespaceConfigs(NAMESPACE, injector);
    Assert.assertTrue(foundNamespaceConfigs.entrySet().containsAll(CONFIGS.entrySet()));
  }
}
