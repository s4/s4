/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 	        http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package org.apache.s4.ft;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * Identifier of PEs. It is used to identify checkpointed PEs in the storage
 * backend.
 * </p>
 * <p>
 * The storage backend is responsible for converting this identifier to whatever
 * internal representation is most suitable for it.
 * </p>
 * <p>
 * This class provides methods for getting a compact String representation of
 * the identifier and for creating an identifier from a String representation.
 * </p>
 * 
 */
public class SafeKeeperId {

    private String prototypeId;
    private String keyed;

    private static final Pattern STRING_REPRESENTATION_PATTERN = Pattern
            .compile("\\[(\\S*)\\];\\[(\\S*)\\]");

    public SafeKeeperId() {
    }

    /**
     * 
     * @param prototypeID
     *            id of the PE as returned by {@link ProcessingElement#getId()
     *            getId()} method
     * @param keyed
     *            keyed attribute(s)
     */
    public SafeKeeperId(String prototypeID, String keyed) {
        super();
        this.prototypeId = prototypeID;
        this.keyed = keyed;
    }

    public SafeKeeperId(String keyAsString) {
        Matcher matcher = STRING_REPRESENTATION_PATTERN.matcher(keyAsString);

        try {
            matcher.find();
            prototypeId = "".equals(matcher.group(1)) ? null : matcher.group(1);
            keyed = "".equals(matcher.group(2)) ? null : matcher.group(2);
        } catch (IndexOutOfBoundsException e) {

        }

    }

    public String getKey() {
        return keyed;
    }

    public String getPrototypeId() {
        return prototypeId;
    }

    public String toString() {
        return "[PROTO_ID];[KEYED] --> " + getStringRepresentation();
    }

    public String getStringRepresentation() {
        return "[" + (prototypeId == null ? "" : prototypeId) + "];["
                + (keyed == null ? "" : keyed) + "]";
    }

    @Override
    public int hashCode() {
        return getStringRepresentation().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        SafeKeeperId other = (SafeKeeperId) obj;
        if (keyed == null) {
            if (other.keyed != null)
                return false;
        } else if (!keyed.equals(other.keyed))
            return false;
        if (prototypeId == null) {
            if (other.prototypeId != null)
                return false;
        } else if (!prototypeId.equals(other.prototypeId))
            return false;
        return true;
    }

}
