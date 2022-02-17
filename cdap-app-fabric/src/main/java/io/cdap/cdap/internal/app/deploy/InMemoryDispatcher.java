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

package io.cdap.cdap.internal.app.deploy;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.common.RuntimeArguments;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.app.deploy.ConfigResponse;
import io.cdap.cdap.app.deploy.Configurator;
import io.cdap.cdap.app.deploy.DispatchResponse;
import io.cdap.cdap.app.deploy.Dispatcher;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.program.Programs;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.lang.jar.ClassLoaderFolder;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentRuntimeInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.AppLaunchInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.AppSpecInfo;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.impersonation.Impersonator;
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

public class InMemoryDispatcher implements Dispatcher {

  private static final String CLUSTER_SCOPE = "cluster";
  private static final String APPLICATION_SCOPE = "app";
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryDispatcher.class);

  private final CConfiguration cConf;
  private final Impersonator impersonator;
  private final ArtifactRepository artifactRepository;
  private final AppLaunchInfo appLaunchInfo;
  private final ProgramRunnerFactory programRunnerFactory;
  private final ConfiguratorFactory configuratorFactory;
  private ProgramRunnerFactory remoteProgramRunnerFactory;
  private final String hostname;
  private final boolean isDistributed;

  @Inject
  public InMemoryDispatcher(CConfiguration cConf,
      ProgramRunnerFactory programRunnerFactory,
      ConfiguratorFactory configuratorFactory,
      Impersonator impersonator,
      ArtifactRepository artifactRepository,
      @Assisted AppLaunchInfo appLaunchInfo,
      @Named(Constants.Service.MASTER_SERVICES_BIND_ADDRESS) InetAddress hostname) {
    this.cConf = cConf;
    this.programRunnerFactory = programRunnerFactory;
    this.configuratorFactory = configuratorFactory;
    this.impersonator = impersonator;
    this.artifactRepository = artifactRepository;
    this.appLaunchInfo = appLaunchInfo;
    this.isDistributed = appLaunchInfo.isDistributed();
    this.hostname = hostname.getCanonicalHostName();
  }

  /**
   * Optional guice injection for the {@link ProgramRunnerFactory} used for remote execution. It is
   * optional because in unit-test we don't have need for that.
   */
  @Inject(optional = true)
  void setRemoteProgramRunnerFactory(
      @Constants.AppFabric.RemoteExecution ProgramRunnerFactory runnerFactory) {
    this.remoteProgramRunnerFactory = runnerFactory;
  }

  @Override
  public ListenableFuture<DispatchResponse> dispatch() {
    SettableFuture<DispatchResponse> result = SettableFuture.create();
    result.set(dispatchPipeline());
    return result;
  }

  public DispatchResponse dispatchPipeline() {
    ProgramDescriptor programDescriptor = appLaunchInfo.getProgramDescriptor();
    RunId runId = appLaunchInfo.getRunId();
    ProgramOptions options = appLaunchInfo.getProgramOptions();
    ProgramId programId = programDescriptor.getProgramId();
    ClusterMode clusterMode = ProgramRunners.getClusterMode(options);

    // Creates the ProgramRunner based on the cluster mode
    ProgramRunner runner = (clusterMode == ClusterMode.ON_PREMISE ? programRunnerFactory
        : Optional.ofNullable(remoteProgramRunnerFactory)
            .orElseThrow(UnsupportedOperationException::new)
    ).create(programId.getType());

    File tempDir = createTempDirectory(programId, runId);
    AtomicReference<Runnable> cleanUpTaskRef = new AtomicReference<>(
        createCleanupTask(tempDir, runner));

    try {
      // Get the artifact details and save it into the program options.
      ArtifactId artifactId = programDescriptor.getArtifactId();
      ArtifactDetail artifactDetail = getArtifactDetail(artifactId);
      ApplicationSpecification appSpec = programDescriptor.getApplicationSpecification();
      ProgramDescriptor newProgramDescriptor = programDescriptor;

      boolean isPreview = Boolean.parseBoolean(
          options.getArguments().getOption(ProgramOptionConstants.IS_PREVIEW, "false"));
      // do the app spec regeneration if the mode is on premise, for isolated mode, the regeneration is done on the
      // runtime environment before the program launch
      // for preview we already have a resolved app spec, so no need to regenerate the app spec again
      if (!isPreview && appSpec != null && ClusterMode.ON_PREMISE.equals(clusterMode)) {
        try {
          ApplicationSpecification generatedAppSpec =
              regenerateAppSpec(artifactDetail, programId, artifactId, appSpec, options);
          appSpec = generatedAppSpec != null ? generatedAppSpec : appSpec;
          newProgramDescriptor = new ProgramDescriptor(programDescriptor.getProgramId(), appSpec);
        } catch (Exception e) {
          LOG.warn("Failed to regenerate the app spec for program {}, using the existing app spec",
              programId);
        }
      }

      ProgramOptions runtimeProgramOptions = updateProgramOptions(artifactId, programId, options,
          runId,
          clusterMode, Iterables.getFirst(artifactDetail.getMeta().getClasses().getApps(), null));

      // Take a snapshot of all the plugin artifacts used by the program
      ProgramOptions optionsWithPlugins = createPluginSnapshot(runtimeProgramOptions, programId,
          tempDir,
          newProgramDescriptor.getApplicationSpecification());

      // Create and run the program
      Program executableProgram = createProgram(cConf, runner, newProgramDescriptor, artifactDetail,
          tempDir);
      cleanUpTaskRef.set(createCleanupTask(cleanUpTaskRef.get(), executableProgram));
      runner.run(executableProgram, optionsWithPlugins);
    } catch (Exception e) {
      LOG.error("Exception while trying to run program", e);
      return new DefaultDispatchResponse(1, null);
    }
    return new DefaultDispatchResponse(0, true);
  }

  /**
   * Regenerates the app spec before the program start
   *
   * @return the regenerated app spec, or null if there is any exception generating the app spec.
   */
  @Nullable
  private ApplicationSpecification regenerateAppSpec(
      ArtifactDetail artifactDetail, ProgramId programId, ArtifactId artifactId,
      ApplicationSpecification existingAppSpec,
      ProgramOptions options) throws InterruptedException, ExecutionException, TimeoutException {
    ApplicationClass appClass = Iterables.getFirst(artifactDetail.getMeta().getClasses().getApps(),
        null);
    if (appClass == null) {
      // This should never happen.
      throw new IllegalStateException(
          String.format("No application class found in artifact '%s' in namespace '%s'.",
              artifactDetail.getDescriptor().getArtifactId(), programId.getNamespace()));
    }

    AppDeploymentInfo deploymentInfo = new AppDeploymentInfo(
        artifactId, artifactDetail.getDescriptor().getLocation(), programId.getNamespaceId(),
        appClass,
        existingAppSpec.getName(), existingAppSpec.getAppVersion(),
        existingAppSpec.getConfiguration(), null, false,
        new AppDeploymentRuntimeInfo(existingAppSpec, options.getUserArguments().asMap(),
            options.getArguments().asMap()));
    Configurator configurator = this.configuratorFactory.create(deploymentInfo);
    ListenableFuture<ConfigResponse> future = configurator.config();
    ConfigResponse response = future.get(120, TimeUnit.SECONDS);

    if (response.getExitCode() == 0) {
      AppSpecInfo appSpecInfo = response.getAppSpecInfo();
      if (appSpecInfo != null && appSpecInfo.getAppSpec() != null) {
        return appSpecInfo.getAppSpec();
      }
    }
    return null;
  }

  /**
   * Creates a {@link Program} for the given {@link ProgramRunner} from the given program jar {@link
   * Location}.
   */
  protected Program createProgram(CConfiguration cConf, ProgramRunner programRunner,
      ProgramDescriptor programDescriptor,
      ArtifactDetail artifactDetail, final File tempDir) throws IOException {

    Location programJarLocation = artifactDetail.getDescriptor().getLocation();
    ClassLoaderFolder classLoaderFolder;
    try {
      // If the program jar is not a directory, take a snapshot of the jar file to avoid mutation.
      if (!programJarLocation.isDirectory()) {
        File targetFile = new File(tempDir, "program.jar");
        try {
          programJarLocation = Locations.toLocation(
              Locations.linkOrCopyOverwrite(programJarLocation,
                  targetFile));
        } catch (FileAlreadyExistsException ex) {
          LOG.warn("Program file {} already exists and can not be replaced.",
              targetFile.getAbsolutePath());
        }
      }
      // Unpack the JAR file
      classLoaderFolder = BundleJarUtil.prepareClassLoaderFolder(programJarLocation,
          () -> Files.createTempDirectory(tempDir.toPath(), "unpacked").toFile());
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      // should not happen
      throw Throwables.propagate(e);
    }
    return Programs.create(cConf, programRunner, programDescriptor, programJarLocation,
        classLoaderFolder.getDir());
  }

  protected ArtifactDetail getArtifactDetail(ArtifactId artifactId) throws Exception {
    return artifactRepository.getArtifact(Id.Artifact.fromEntityId(artifactId));
  }

  /**
   * Creates a local temporary directory for this program run.
   */
  private File createTempDirectory(ProgramId programId, RunId runId) {
    File tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
        cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    File dir = new File(tempDir, String.format("%s.%s.%s.%s.%s",
        programId.getType().name().toLowerCase(),
        programId.getNamespace(), programId.getApplication(),
        programId.getProgram(), runId.getId()));
    dir.mkdirs();
    return dir;
  }

  private Runnable createCleanupTask(final Object... resources) {
    AtomicBoolean executed = new AtomicBoolean();
    return () -> {
      if (!executed.compareAndSet(false, true)) {
        return;
      }
      List<Object> resourceList = new ArrayList<>(Arrays.asList(resources));
      Collections.reverse(resourceList);
      for (Object resource : resourceList) {
        if (resource == null) {
          continue;
        }

        try {
          if (resource instanceof File) {
            File file = (File) resource;
            if (file.isDirectory()) {
              DirUtils.deleteDirectoryContents(file);
            } else {
              file.delete();
            }
          } else if (resource instanceof Closeable) {
            Closeables.closeQuietly((Closeable) resource);
          } else if (resource instanceof Runnable) {
            ((Runnable) resource).run();
          }
        } catch (Throwable t) {
          LOG.warn("Exception when cleaning up resource {}", resource, t);
        }
      }
    };
  }

  /**
   * Updates the given {@link ProgramOptions} and return a new instance. It copies the {@link
   * ProgramOptions}. Then it adds all entries returned by {@link #getExtraProgramOptions()}
   * followed by adding the {@link RunId} to the system arguments.
   *
   * Also scope resolution will be performed on the user arguments on the application and program.
   *
   * @param programId the program id
   * @param options The {@link ProgramOptions} in which the RunId to be included
   * @param runId The RunId to be included
   * @param clusterMode clustermode for the program run
   * @param applicationClass application class for the program
   * @return the copy of the program options with RunId included in them
   */
  private ProgramOptions updateProgramOptions(ArtifactId artifactId, ProgramId programId,
      ProgramOptions options, RunId runId, ClusterMode clusterMode,
      ApplicationClass applicationClass) {
    // Build the system arguments
    Map<String, String> systemArguments = new HashMap<>(options.getArguments().asMap());
    // don't add these system arguments if they're already there
    // this can happen if this is a program within a workflow, and the workflow already added these arguments
    Map<String, String> extraProgramOptions =
        isDistributed ? getDistributedExtraProgramOptions() : getExtraProgramOptions();
    for (Map.Entry<String, String> extraOption : extraProgramOptions.entrySet()) {
      systemArguments.putIfAbsent(extraOption.getKey(), extraOption.getValue());
    }
    systemArguments.putIfAbsent(ProgramOptionConstants.RUN_ID, runId.getId());
    systemArguments.putIfAbsent(ProgramOptionConstants.ARTIFACT_ID,
        Joiner.on(':').join(artifactId.toIdParts()));
    if (clusterMode == ClusterMode.ISOLATED) {
      systemArguments.putIfAbsent(ProgramOptionConstants.APPLICATION_CLASS,
          applicationClass.getClassName());
    }

    // Resolves the user arguments
    // First resolves at the cluster scope if the cluster.name is not empty
    String clusterName = options.getArguments().getOption(Constants.CLUSTER_NAME);
    Map<String, String> userArguments = options.getUserArguments().asMap();
    if (!Strings.isNullOrEmpty(clusterName)) {
      userArguments = RuntimeArguments.extractScope(CLUSTER_SCOPE, clusterName, userArguments);
    }
    // Then resolves at the application scope
    userArguments = RuntimeArguments.extractScope(APPLICATION_SCOPE, programId.getApplication(),
        userArguments);
    // Then resolves at the program level
    userArguments = RuntimeArguments.extractScope(programId.getType().getScope(),
        programId.getProgram(),
        userArguments);

    return new SimpleProgramOptions(options.getProgramId(), new BasicArguments(systemArguments),
        new BasicArguments(userArguments), options.isDebug());
  }

  /**
   * Return the copy of the {@link ProgramOptions} including locations of plugin artifacts in it.
   *
   * @param options the {@link ProgramOptions} in which the locations of plugin artifacts needs to
   * be included
   * @param programId Id of the Program
   * @param tempDir Temporary Directory to create the plugin artifact snapshot
   * @param appSpec program's Application Specification
   * @return the copy of the program options with locations of plugin artifacts included in them
   */
  private ProgramOptions createPluginSnapshot(ProgramOptions options, ProgramId programId,
      File tempDir,
      @Nullable ApplicationSpecification appSpec) throws Exception {
    // appSpec is null in an unit test
    if (appSpec == null) {
      return options;
    }

    Set<String> files = Sets.newHashSet();
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.putAll(options.getArguments().asMap());
    for (Map.Entry<String, Plugin> pluginEntry : appSpec.getPlugins().entrySet()) {
      Plugin plugin = pluginEntry.getValue();
      File destFile = new File(tempDir, Artifacts.getFileName(plugin.getArtifactId()));
      // Skip if the file has already been copied.
      if (!files.add(destFile.getName())) {
        continue;
      }

      try {
        ArtifactId artifactId = Artifacts.toProtoArtifactId(programId.getNamespaceId(),
            plugin.getArtifactId());
        ArtifactDetail artifactDetail = getArtifactDetail(artifactId);
        if (isDistributed) {
          copyArtifactInDistributed(artifactId, artifactDetail, destFile);
        } else {
          copyArtifact(artifactId, artifactDetail, destFile);
        }
      } catch (ArtifactNotFoundException e) {
        throw new IllegalArgumentException(
            String.format("Artifact %s could not be found", plugin.getArtifactId()), e);
      }
    }
    LOG.debug("Plugin artifacts of {} copied to {}", programId, tempDir.getAbsolutePath());
    builder.put(ProgramOptionConstants.PLUGIN_DIR, tempDir.getAbsolutePath());
    return new SimpleProgramOptions(options.getProgramId(), new BasicArguments(builder.build()),
        options.getUserArguments(), options.isDebug());
  }

  /**
   * Copies the artifact jar to the given target file.
   *
   * @param artifactId artifact id of the artifact to be copied
   * @param artifactDetail detail information of the artifact to be copied
   * @param targetFile target file to copy to
   * @throws IOException if the copying failed
   */
  private void copyArtifact(ArtifactId artifactId,
      ArtifactDetail artifactDetail, File targetFile) throws IOException {
    Locations.linkOrCopy(artifactDetail.getDescriptor().getLocation(), targetFile);
  }

  private void copyArtifactInDistributed(ArtifactId artifactId,
      final ArtifactDetail artifactDetail, final File targetFile) throws IOException {
    try {
      impersonator.doAs(artifactId, () -> {
        Locations.linkOrCopy(artifactDetail.getDescriptor().getLocation(), targetFile);
        return null;
      });
    } catch (FileAlreadyExistsException ex) {
      LOG.warn("Artifact file {} already exists.", targetFile.getAbsolutePath());
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, IOException.class);
      // should not happen
      throw Throwables.propagate(e);
    }
  }

  private Map<String, String> getDistributedExtraProgramOptions() {
    return Collections.emptyMap();
  }

  private Map<String, String> getExtraProgramOptions() {
    return Collections.singletonMap(ProgramOptionConstants.HOST, hostname);
  }
}
