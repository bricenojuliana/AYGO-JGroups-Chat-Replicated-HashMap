package workshop;

import java.io.Serializable;
import java.util.UUID;

/**
 * One replicated "/store key value" operation.
 * The id is globally unique, so a node can recognise an operation it has already applied.
 */
public record StoreCommand(String id, String key, String value) implements Serializable {

    /** New operation created by a user: id = originNode-randomUUID. */
    public static StoreCommand create(String origin, String key, String value) {
        return new StoreCommand(origin + "-" + UUID.randomUUID(), key, value);
    }
}
