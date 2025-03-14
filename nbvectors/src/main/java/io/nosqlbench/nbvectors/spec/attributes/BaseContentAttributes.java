package io.nosqlbench.nbvectors.spec.attributes;

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


/// This record type captures attribute requirements for the base content dataset
/// @param media_type the media type of the content, such as "text/plain" or "image/jpeg"
/// @param count the number of content items
public record BaseContentAttributes(
    String media_type,
    long count
) {
}
