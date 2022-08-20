package org.apache.arrow.table;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;

/**
 * MutableCursor is a positionable, mutable cursor backed by a {@link MutableTable}.
 *
 * If a row in a table is marked as deleted, it is skipped when iterating.
 */
public class MutableCursor extends ImmutableCursor {

    /**
     * Constructs a new MutableCursor backed by the given table
     * @param table the table that this MutableCursor object represents
     */
    public MutableCursor(MutableTable table) {
        super(table);
    }

    private MutableTable getTable() {
        return (MutableTable) table;
    }

    /**
     * Moves this MutableCursor to the given 0-based row index
     * @return this Cursor for chaining
     **/
    public MutableCursor at(int rowNumber) {
        super.at(rowNumber);
        return this;

    }

    public MutableCursor setNull(String columnName) {
        FieldVector v = table.getVector(columnName);
        // TODO: Real implementation after fixing setNull issue
        if (v instanceof IntVector) {
            ((IntVector) v).setNull(getRowNumber());
        }
        return this;
    }

    /**
     * Mark the current row as deleted
     *
     * @return this row for chaining
     */
    public MutableCursor delete() {
        getTable().markRowDeleted(getRowNumber());
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is * thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setInt(int columnIndex, int value) {
        IntVector v = (IntVector) table.getVector(columnIndex);
        v.set(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is * thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setInt(String columnName, int value) {
        IntVector v = (IntVector) table.getVector(columnName);
        v.set(getRowNumber(), value);
        return this;
    }
}
