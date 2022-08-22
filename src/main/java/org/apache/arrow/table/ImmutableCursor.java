package org.apache.arrow.table;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * MutableCursor is a positionable, immutable cursor backed by a {@link MutableTable}.
 * If a row in a table is marked as deleted, it is skipped when iterating.
 */
public class ImmutableCursor implements Iterator<ImmutableCursor> {

    /** The table we're enumerating */
    protected final BaseTable table;

    /** the current row number */
    private int rowNumber = -1;

    /** Indicates whether the next non-deleted row has been calculated yet */
    private boolean nextRowSet;

    /** An iterator that returns every row, deleted or not. We wrap it to get only the non-deleted ones */
    private final Iterator<Integer> iterator = intIterator();

    /**
     * Returns the standard character set to use for decoding strings. Can be overridden for individual columns
     * by providing the CharSet as an argument in the getter
     */
    private Charset standardCharacterSet = StandardCharsets.UTF_8;

    /**
     * Constructs a new MutableCursor backed by the given table.
     * @param table the table that this MutableCursor object represents
     */
    public ImmutableCursor(BaseTable table) {
        this.table = table;
    }

    /**
     * Constructs a new MutableCursor backed by the given table
     * @param table     the table that this MutableCursor object represents
     * @param charset   the standard charset for decoding bytes into strings. This can be set for individual columns
     */
    public ImmutableCursor(BaseTable table, Charset charset) {
        this.table = table;
        this.standardCharacterSet = charset;
    }

    /**
     * Moves this MutableCursor to the given 0-based row index
     * @return this Cursor for chaining
     **/
    public ImmutableCursor at(int rowNumber) {
        this.rowNumber = rowNumber;
        return this;
    }

    /** Returns true if the value at columnName is null, and false otherwise */
    public boolean isNull(String columnName) {
        ValueVector vector = table.getVector(columnName);
        return vector.isNull(rowNumber);
    }

    /**
     * Returns an int from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present in the MutableCursor and an IllegalArgumentException is thrown if it
     * has a different type
     */
    public int getInt(String columnName) {
        IntVector vector = (IntVector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns an int from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present and an IllegalArgumentException is thrown if it
     * has a different type
     */
    public int getInt(int columnIndex) {
        IntVector vector = (IntVector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns a String from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present in the MutableCursor and an IllegalArgumentException is thrown if it
     * has a different type
     *
     * StandardCharsets.UTF_8 is used as the charset
     */
    public String getVarChar(String columnName) {
        VarCharVector vector = (VarCharVector) table.getVector(columnName);

        return new String(vector.get(rowNumber), standardCharacterSet);
    }

    /**
     * Returns a String from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present in the MutableCursor and an IllegalArgumentException is thrown if it
     * has a different type
     *
     * @param vectorName    the name of the FieldVector holding the value
     * @param charset       the charset to use for decoding the bytes
     */
    public String getVarChar(String vectorName, Charset charset) {
        VarCharVector vector = (VarCharVector) table.getVector(vectorName);

        return new String(vector.get(rowNumber), charset);
    }

    /**
     * Returns a String from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present in the MutableCursor and an IllegalArgumentException is thrown if it
     * has a different type
     */
    public String getVarChar(int columnIndex) {
        VarCharVector vector = (VarCharVector) table.getVector(columnIndex);
        return new String(vector.get(rowNumber), standardCharacterSet);
    }

    public String getVarChar(int columnIndex, Charset charset) {
        VarCharVector vector = (VarCharVector) table.getVector(columnIndex);
        return new String(vector.get(rowNumber), charset);
    }

    /**
     * Returns true if there is at least one more non-deleted row in the table that has yet to be processed
     */
    @Override
    public boolean hasNext() {
        return nextRowSet || setNextObject();
    }

    /**
     * Returns the next non-deleted row in the table
     *
     * @throws NoSuchElementException if there are no more rows
     */
    @Override
    public ImmutableCursor next() {
        if (!nextRowSet && !setNextObject()) {
            throw new NoSuchElementException();
        }
        nextRowSet = false;
        return this;
    }

    /**
     * Set rowNumber to the next non-deleted row. If there are no more
     * rows return false. Otherwise, return true.
     */
    private boolean setNextObject() {
        while (iterator.hasNext()) {
            final int row = iterator.next();
            if (!rowIsDeleted(row)) {
                rowNumber = row;
                nextRowSet = true;
                return true;
            }
        }
        return false;
    }

    /**
     * TODO: When used with an ImmutableTable there would be no deleted rows, so this method could be used, but
     *   the deletion support is needed when it's needed as an immutable iterator for MutableTable.
     *
     * Returns new internal iterator that processes every row, deleted or not.
     * Users should use the wrapping next() and hasNext() methods rather than using this iterator directly,
     * unless you want to see any deleted rows.
     */
    private Iterator<Integer> intIterator() {
        return new Iterator<>() {
            int row = -1;

            @Override
            public boolean hasNext() {
                return row < table.getRowCount() - 1;
            }

            @Override
            public Integer next() {
                row++;
                return row;
            }
        };
    }

    public int getRowNumber() {
        return rowNumber;
    }

    private boolean rowIsDeleted(int rowNumber) {
        return table.isDeletedRow(rowNumber);
    }
}
