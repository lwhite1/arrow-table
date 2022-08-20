package org.apache.arrow.table;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;

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
     * Constructs a new MutableCursor backed by the given table
     * @param table the table that this MutableCursor object represents
     */
    public ImmutableCursor(BaseTable table) {
        this.table = table;
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
     * Returns an int from this MutableCursor at the column of the given name. An IllegalStateException is
     * thrown if the column is not present in the MutableCursor and an IllegalArgumentException is thrown if it
     * has a different type
     */
    public int getInt(String columnName) {
        IntVector vector = (IntVector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns an int from this MutableCursor at the column with the given index. An IllegalStateException is
     * thrown if the column is not present in the MutableCursor and an IllegalArgumentException is thrown if it
     * has a different type
     */
    public int getInt(int columnIndex) {
        IntVector vector = (IntVector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns true if there is at least one more non-deleted row in the table
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
     * Returns new internal iterator that processes every row, deleted or not
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
