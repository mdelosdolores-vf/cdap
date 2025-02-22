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

package io.cdap.cdap.gateway.handlers;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.implementation.HealthCheckImplementation;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.security.InstancePermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Health Check HTTP Handler.
 */
@ApplicationScoped
@Path(Constants.Gateway.API_VERSION_3)
public class HealthCheckHttpHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HealthCheckHttpHandler.class);
  private static final Gson GSON = new Gson();
  private final HealthCheckImplementation healthCheckImplementation;
  private final ContextAccessEnforcer contextAccessEnforcer;

  @Inject
  HealthCheckHttpHandler(HealthCheckImplementation healthCheckImplementation,
                         ContextAccessEnforcer contextAccessEnforcer) {
    this.healthCheckImplementation = healthCheckImplementation;
    this.contextAccessEnforcer = contextAccessEnforcer;
  }

  @GET
  @Path("/health/{serviceName}")
  public void call(HttpRequest request, HttpResponder responder, @PathParam("serviceName") String serviceName) {
    // ensure the user has authentication to get health check
    contextAccessEnforcer.enforce(InstanceId.SELF, InstancePermission.HEALTH_CHECK);
    HealthCheckResponse healthCheckResponse = healthCheckImplementation.collect();
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(healthCheckResponse.getData()));
  }
}
