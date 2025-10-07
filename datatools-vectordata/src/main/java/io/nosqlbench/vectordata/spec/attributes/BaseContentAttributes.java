package io.nosqlbench.vectordata.spec.attributes;

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


/// This class captures attribute requirements for the base content dataset
public class BaseContentAttributes {
    /// the media type of the content, such as "text/plain" or "image/jpeg"
    private final String media_type;
    /// the number of content items
    private final long count;

    public BaseContentAttributes(String media_type, long count) {
        this.media_type = media_type;
        this.count = count;
    }

    public String media_type() {
        return media_type;
    }

    public long count() {
        return count;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        BaseContentAttributes that = (BaseContentAttributes) obj;
        return count == that.count && java.util.Objects.equals(media_type, that.media_type);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(media_type, count);
    }

    @Override
    public String toString() {
        return "BaseContentAttributes{" +
                "media_type='" + media_type + '\'' +
                ", count=" + count +
                '}';
    }
}
