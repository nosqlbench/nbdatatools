package io.nosqlbench.vectordata.status;

/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collections;
import java.util.Map;

/**
 * Placeholder for backward compatibility.
 * The original MerklePainterEvent was removed with the legacy merkle v1 system.
 * New implementations should use the merklev2 package.
 * 
 * @deprecated Use merklev2 package for new merkle implementations
 */
@Deprecated
public enum MerklePainterEvent implements EventType {
    // Legacy compatibility placeholder
    LEGACY_EVENT(EventType.Level.INFO);
    
    private final EventType.Level level;
    
    MerklePainterEvent(EventType.Level level) {
        this.level = level;
    }
    
    @Override
    public EventType.Level getLevel() {
        return level;
    }
    
    @Override
    public Map<String, Class<?>> getRequiredParams() {
        return Collections.emptyMap();
    }
    
    // Legacy compatibility method
    public Map<String, String> getParamDescriptions() {
        return Collections.emptyMap();
    }
}