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
 */

package io.cdap.cdap.internal.app.worker;

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.app.deploy.DispatchResponse;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactory;
import io.cdap.cdap.internal.app.deploy.InMemoryDispatcher;
import io.cdap.cdap.internal.app.deploy.pipeline.AppLaunchInfo;
import io.cdap.cdap.internal.app.runtime.artifact.ApplicationClassCodec;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.RequirementsCodec;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.impersonation.Impersonator;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class DispatchTask implements RunnableTask {

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(
          new GsonBuilder())
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .registerTypeAdapter(ApplicationClass.class, new ApplicationClassCodec())
      .registerTypeAdapter(Requirements.class, new RequirementsCodec())
      .create();

  private final CConfiguration cConf;

  @Inject
  DispatchTask(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  public void run(RunnableTaskContext context) throws Exception {
    AppLaunchInfo appLaunchInfo = GSON.fromJson(context.getParam(), AppLaunchInfo.class);

    Injector injector = Guice.createInjector(
        new ConfigModule(cConf),
        new LocalLocationModule(),
        new ConfiguratorTaskModule(),
        new AuthenticationContextModules().getMasterWorkerModule()
    );
    DispatchResponse response = injector.getInstance(DispatchTaskRunner.class)
        .dispatch(appLaunchInfo);
    if (response.getExitCode() == 0 && response.isSuccessfulLaunch()) {
      context.setTerminateOnComplete(true);
    }
    context.writeResult(GSON.toJson(response).getBytes(StandardCharsets.UTF_8));
  }

  private static class DispatchTaskRunner {

    private final CConfiguration cConf;
    private final ProgramRunnerFactory programRunnerFactory;
    private final ConfiguratorFactory configuratorFactory;
    private final Impersonator impersonator;
    private final ArtifactRepository artifactRepository;
    private final InetAddress hostname;

    @Inject
    public DispatchTaskRunner(CConfiguration cConf,
        ProgramRunnerFactory programRunnerFactory,
        ConfiguratorFactory configuratorFactory,
        Impersonator impersonator,
        ArtifactRepository artifactRepository,
        @Named(Constants.Service.MASTER_SERVICES_BIND_ADDRESS) InetAddress hostname) {
      this.cConf = cConf;
      this.programRunnerFactory = programRunnerFactory;
      this.configuratorFactory = configuratorFactory;
      this.impersonator = impersonator;
      this.artifactRepository = artifactRepository;
      this.hostname = hostname;
    }

    public DispatchResponse dispatch(AppLaunchInfo appLaunchInfo) throws Exception {
      InMemoryDispatcher dispatcher = new InMemoryDispatcher(cConf, programRunnerFactory,
          configuratorFactory, impersonator, artifactRepository, appLaunchInfo, hostname);
      try {
        return dispatcher.dispatch().get(120, TimeUnit.SECONDS);
      } catch (Exception e) {
        // We don't need the ExecutionException being reported back to the RemoteTaskExecutor, hence only
        // propagating the actual cause.
        Throwables.propagateIfPossible(e.getCause(), Exception.class);
        throw Throwables.propagate(e.getCause());
      }
    }
  }
}
