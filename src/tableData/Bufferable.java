package tableData;

import exceptions.CustomExceptions.*;

import java.io.IOException;

public abstract class Bufferable {

    public int index;
    private int numFreezes = 0;

    /**
     * Returns the table name that this Bufferable belongs to
     * @return The table name
     */
    public abstract String getTableName();

    /**
     * Checks if a Bufferable belongs to a given schema
     * @param schema The schema being matched against
     * @return `true` if this object belongs to the specified table; `false` otherwise
     */
    public boolean matchesSchema(TableSchema schema) {
        return schema.name.equals(this.getTableName());
    }

    /**
     * Checks if this object belongs to a specific table and has a specific ID
     * <br>
     * <b>NOTE:</b> Bufferables of different types may have the same id. Whenever
     * fetching a specific Page or BPlusNode from the buffer, a type check is
     * required as well
     * @param schema The TableSchema of the table being fetched from
     * @param id The ID of the Bufferable being fetched
     * @return `true` if this object matches the schema and id
     */
    public boolean match(TableSchema schema, int id) {
        return matchesSchema(schema) && index == id;
    }

    /**
     * Writes the contents of this Bufferable to disk
     */
    public abstract void save() throws IOException;

    /**
     * Checks if this Bufferable is "frozen", meaning an in progress command has marked it to
     * not be purged from the buffer. This status should always be removed at the end of processing
     * the command
     * @return `true` if this Bufferable is frozen
     */
    public boolean isFrozen() {
        return numFreezes != 0;
    }

    /**
     * "Freezes" this Bufferable, meaning it will not be purged from the buffer. Must be
     */
    public void freeze() {
        numFreezes += 1;
    }

    /**
     * Releases a freeze on this Bufferable. Does not necessarily fully unfreeze the Page
     * if there are other outstanding freezes.
     * @throws PageFreezeException if page has no outstanding freezes
     */
    public void unfreeze() {
        if (numFreezes == 0) {
            throw new PageFreezeException("Attempted to unfreeze page which wasn't frozen: `" + this + "`");
        }
        numFreezes -= 1;
    }
}
