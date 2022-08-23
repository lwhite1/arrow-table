package org.apache.arrow.table;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public abstract class BaseCursor {

    // TODO: pull other fields and methods up


    /** The table we're enumerating */
    protected final BaseTable table;

    /**
     * Returns the standard character set to use for decoding strings. Can be overridden for individual columns
     * by providing the {@link Charset} as an argument in the getter
     */
    private Charset defaultCharacterSet = StandardCharsets.UTF_8;

    /**
     * Constructs a new BaseCursor backed by the given table.
     * @param table the table that this MutableCursor object represents
     */
    public BaseCursor(BaseTable table) {
        this.table = table;
    }

    /**
     * Constructs a new BaseCursor backed by the given table
     * @param table     the table that this cursor represents
     * @param charset   the standard charset for decoding bytes into strings. Note: This can be overridden for
     *                  individual columns
     */
    public BaseCursor(BaseTable table, Charset charset) {
        this.table = table;
        this.defaultCharacterSet = charset;
    }


    public Charset getDefaultCharacterSet() {
        return defaultCharacterSet;
    }

}
