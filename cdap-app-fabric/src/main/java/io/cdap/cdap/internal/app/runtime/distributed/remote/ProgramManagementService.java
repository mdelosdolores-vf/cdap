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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.AbstractRetryableScheduledService;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * A service that periodically checks if the program is still running.
 */
final class ProgramManagementService extends AbstractRetryableScheduledService { // 0 Retryable or not? if yes then override other methods

  private static final Logger LOG = LoggerFactory.getLogger(ProgramManagementService.class);

  private final TransactionRunner transactionRunner;
  private final ProgramId programId;
  private final RemoteProcessController processController;
  private final ProgramStateWriter programStateWriter;
  private final long pollTimeMillis;
  private long nextCheckRunningMillis;

  /**
   * Constructor.
   * @param cConf the {@link RetryStrategy} for determining how to retry when there is exception raised
   * @param programId
   * @param processController
   * @param programStateWriter
   */
  protected ProgramManagementService(CConfiguration cConf, TransactionRunner transactionRunner,
                                     ProgramId programId, RemoteProcessController processController,
                                     ProgramStateWriter programStateWriter) {
    super(RetryStrategies
            .fixDelay(cConf.getLong(Constants.AppFabric.SYSTEM_PROGRAM_SCAN_INTERVAL_SECONDS), TimeUnit.SECONDS)); // ?1
    this.transactionRunner = transactionRunner;
    this.programId = programId;
    this.processController = processController;
    this.programStateWriter = programStateWriter;
    this.pollTimeMillis = cConf.getLong(Constants.RuntimeMonitor.POLL_TIME_MS);
  }

  @Override
  protected long runTask() throws Exception {
    long now = System.currentTimeMillis();
      TransactionRunners.run(transactionRunner, context -> {
      AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
      appMetadataStore.scanActiveRuns(programId, runRecordDetail -> {
        if (runRecordDetail.getStatus() == ProgramRunStatus.STOPPING) {
          // 2 what if stoppingTs is null?
          if (TimeUnit.MILLISECONDS.toSeconds(now) >= runRecordDetail.getStoppingTs()) {
            // Periodically check if the program that has received a stopping command is still running
            try {
              if (processController.isRunning()) {
                //
                LOG.debug("Program {} is not running", programId);
                processController.terminate();
                // 3 programRunId or programId in constructor? how to get one from the other?
                // 4 have gracefulShutdownSecs in RunRecord or change state writer to take in terminateTs?
                programStateWriter.stop(null, -1);
              }
            } catch (Exception e) {
              LOG.error("Error while fetching process controller"); // 4 what to do in this case?
            }
          }
        }
      });
    });
    nextCheckRunningMillis = now + pollTimeMillis * 10;
    return pollTimeMillis;
  }
}
