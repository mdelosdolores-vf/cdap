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

package io.cdap.cdap.internal.app.deploy.pipeline;

import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramOptions;

public class AppLaunchInfo {

  private final Program program;
  private final ProgramOptions programOptions;

  public AppLaunchInfo(Program program, ProgramOptions programOptions) {
    this.program = program;
    this.programOptions = programOptions;
  }

  public Program getProgram() {
    return program;
  }

  public ProgramOptions getProgramOptions() {
    return programOptions;
  }
}
