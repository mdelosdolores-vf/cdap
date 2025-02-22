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

package io.cdap.cdap.common.guice;

import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.DefaultRemoteAuthenticatorProvider;
import io.cdap.cdap.common.internal.remote.NoOpRemoteAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteAuthenticatorExtensionLoader;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;

/**
 * Provides Guice bindings for {@link RemoteAuthenticator}.
 */
public final class RemoteAuthenticatorModules {

  private RemoteAuthenticatorModules() {}

  /**
   * Returns the default bindings for the {@link RemoteAuthenticator}.
   * @return A module with {@link RemoteAuthenticator} bindings
   */
  public static Module getDefaultModule() {
    return getDefaultModule(Constants.RemoteAuthenticator.REMOTE_AUTHENTICATOR_NAME);
  }

  /**
   * Returns the default bindings for the {@link RemoteAuthenticator}.
   *
   * @param remoteAuthenticatorNameKey A {@link io.cdap.cdap.common.conf.CConfiguration} config which should be used
   *                                   in place of system-wide remote authenticator bindings. If the resulting
   *                                   authenticator key is null, it will fall back to the system-wide config.
   * @return A module with {@link RemoteAuthenticator} bindings
   */
  public static Module getDefaultModule(String remoteAuthenticatorNameKey) {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(String.class)
          .annotatedWith(Names.named(DefaultRemoteAuthenticatorProvider.AUTHENTICATOR_NAME_KEY))
          .toInstance(remoteAuthenticatorNameKey);
        bind(RemoteAuthenticator.class).toProvider(DefaultRemoteAuthenticatorProvider.class);
        bind(RemoteAuthenticatorExtensionLoader.class).in(Scopes.SINGLETON);
        expose(RemoteAuthenticator.class);
      }
    };
  }

  /**
   * Returns a no-op binding for the {@link RemoteAuthenticator} for testing.
   */
  public static Module getNoOpModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(RemoteAuthenticator.class).to(NoOpRemoteAuthenticator.class);
        expose(RemoteAuthenticator.class);
      }
    };
  }
}
