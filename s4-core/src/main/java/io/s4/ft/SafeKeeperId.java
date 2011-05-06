package io.s4.ft;

import io.s4.processor.ProcessingElement;

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
