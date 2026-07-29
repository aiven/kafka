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
package org.apache.kafka.clients.admin;

import java.util.Map;

/**
 * Options for {@link AdminClient#updateFeatures(Map, UpdateFeaturesOptions)}.
 */
public class UpdateFeaturesOptions extends AbstractOptions<UpdateFeaturesOptions> {
    private boolean validateOnly = false;
    private boolean ignoreStaleControllerRegistrations = false;

    public boolean validateOnly() {
        return validateOnly;
    }

    public UpdateFeaturesOptions validateOnly(boolean validateOnly) {
        this.validateOnly = validateOnly;
        return this;
    }

    /**
     * Returns whether feature validation on the controller should ignore controller registrations
     * that are not part of the current live voter set.
     */
    public boolean ignoreStaleControllerRegistrations() {
        return ignoreStaleControllerRegistrations;
    }

    /**
     * Set whether feature validation on the controller should ignore controller registrations that
     * are not part of the current live voter set.
     *
     * This is a recovery option for clusters where removed controllers are still registered in the
     * metadata log and block feature upgrades. Controllers that are running but are not voters, such
     * as observer controllers, are ignored as well. Setting this to true requires a controller that
     * supports version 3 or later of the UpdateFeatures RPC; otherwise the request fails with
     * {@link org.apache.kafka.common.errors.UnsupportedVersionException}.
     */
    public UpdateFeaturesOptions ignoreStaleControllerRegistrations(boolean ignoreStaleControllerRegistrations) {
        this.ignoreStaleControllerRegistrations = ignoreStaleControllerRegistrations;
        return this;
    }
}
