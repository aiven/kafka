/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.mirror.rest.resources;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.HerderRequestHandler;
import org.apache.kafka.connect.runtime.rest.InternalRequestSignature;
import org.apache.kafka.connect.runtime.rest.resources.ConnectResource;
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class InternalConnectorsResource implements ConnectResource {

    private static final Logger log = LoggerFactory.getLogger(InternalConnectorsResource.class);

    private static final TypeReference<List<Map<String, String>>> TASK_CONFIGS_TYPE =
            new TypeReference<List<Map<String, String>>>() { };

    private final Map<SourceAndTarget, Herder> herders;
    private final HerderRequestHandler requestHandler;

    public InternalConnectorsResource(Map<SourceAndTarget, Herder> herders, AbstractConfig serverConfig) {
        this.herders = herders;
        this.requestHandler = new HerderRequestHandler(serverConfig, DEFAULT_REST_REQUEST_TIMEOUT_MS);
    }

    @Override
    public void requestTimeout(long requestTimeoutMs) {
        requestHandler.requestTimeoutMs(requestTimeoutMs);
    }

    @POST
    @Path("/{source}/{target}/connectors/{connector}/tasks")
    @Operation(hidden = true, summary = "This operation is only for inter-worker communications")
    public void putTaskConfigs(
            final @PathParam("source") String source,
            final @PathParam("target") String target,
            final @PathParam("connector") String connector,
            final @Context HttpHeaders headers,
            final @QueryParam("forward") Boolean forward,
            final byte[] requestBody) throws Throwable {
        List<Map<String, String>> taskConfigs = new ObjectMapper().readValue(requestBody, TASK_CONFIGS_TYPE);
        FutureCallback<Void> cb = new FutureCallback<>();
        herder(source, target).putTaskConfigs(connector, taskConfigs, cb, InternalRequestSignature.fromHeaders(requestBody, headers));
        requestHandler.completeOrForwardRequest(
                cb,
                source + "/" + target + "/connectors/" + connector + "/tasks",
                "POST",
                headers,
                taskConfigs,
                forward
        );
    }

    @PUT
    @Path("/{source}/{target}/connectors/{connector}/fence")
    @Operation(hidden = true, summary = "This operation is only for inter-worker communications")
    public void fenceZombies(
            final @PathParam("source") String source,
            final @PathParam("target") String target,
            final @PathParam("connector") String connector,
            final @Context HttpHeaders headers,
            final @QueryParam("forward") Boolean forward,
            final byte[] requestBody) throws Throwable {
        FutureCallback<Void> cb = new FutureCallback<>();
        herder(source, target).fenceZombieSourceTasks(connector, cb, InternalRequestSignature.fromHeaders(requestBody, headers));
        requestHandler.completeOrForwardRequest(
                cb,
                source + "/" + target + "/connectors/" + connector + "/fence",
                "PUT",
                headers,
                requestBody,
                forward
        );
    }

    private Herder herder(String source, String target) {
        Herder result = herders.get(new SourceAndTarget(source, target));
        if (result == null) {
            log.debug("Failed to find herder for source '{}' and target '{}'", source, target);
            throw new NotFoundException("No replication flow found for source '" + source + "' and target '" + target + "'");
        }
        return result;
    }
}
