package org.apache.arrow.table;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;

import javax.annotation.Nullable;
import java.nio.charset.Charset;

/**
 * MutableCursor is a positionable, mutable cursor backed by a {@link MutableTable}.
 *
 * If a row in a table is marked as deleted, it is skipped when iterating.
 *
 * TODO: map a ValueHolder for each vector
 */
public class MutableCursor extends Cursor {

    /**
     * DictionaryProvider for any Dictionary-encoded vectors in the Table. This may be null if no vectors are encoded
     */
    private DictionaryProvider dictionaryProvider;

    /**
     * Constructs a new MutableCursor backed by the given table
     *
     * @param table the table that this MutableCursor object represents
     */
    public MutableCursor(MutableTable table) {
        super(table);
        this.dictionaryProvider = table.getDictionaryProvider();
    }

    /**
     * Constructs a new MutableCursor backed by the given table
     *
     * @param table   the table that this MutableCursor object represents
     * @param charset the default charset for encoding/decoding strings
     */
    public MutableCursor(MutableTable table, Charset charset) {
        super(table, charset);
    }

    /**
     * Returns the table that backs this cursor
     */
    private MutableTable getTable() {
        return (MutableTable) table;
    }

    /**
     * Moves this MutableCursor to the given 0-based row index
     * TODO: Determine the best way to handle case where the row requested has been deleted.
     *
     * @return this Cursor for method chaining
     **/
    public MutableCursor at(int rowNumber) {
        super.at(rowNumber);
        return this;
    }

    /**
     * Sets a null value in the named vector at the current row.
     *
     * @param columnName The name of the column to update
     * @return this Cursor for method chaining
     */
    public MutableCursor setNull(String columnName) {
        FieldVector v = table.getVector(columnName);
        // TODO: Real implementation (without casts) after fixing setNull issue
        if (v instanceof IntVector) {
            ((IntVector) v).setNull(getRowNumber());
        }
        return this;
    }

    /**
     * Sets a null value in the named vector at the current row.
     *
     * @param columnIndex The index of the column to update
     * @return this Cursor for method chaining
     */
    public MutableCursor setNull(int columnIndex) {
        FieldVector v = table.getVector(columnIndex);
        // TODO: Real implementation (without casts) after fixing setNull issue
        if (v instanceof IntVector) {
            ((IntVector) v).setNull(getRowNumber());
        }
        return this;
    }

    /**
     * Marks the current row as deleted.
     * TODO: should we add an un-delete method. See issue with at()
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

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is * thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setVarChar(int columnIndex, String value) {
        VarCharVector v = (VarCharVector) table.getVector(columnIndex);
        v.set(getRowNumber(), value.getBytes(getDefaultCharacterSet()));
        // TODO: Finish implementation (because this doesn't handle dictionaries, etc)
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is * thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @param vectorName The name of the vector to modify
     * @param value      The new value for the current row
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setVarChar(String vectorName, String value) {
        VarCharVector v = (VarCharVector) table.getVector(vectorName);
        Dictionary dictionary = dictionary(v);
        if (dictionary != null) {
            v.set(getRowNumber(), value.getBytes(getDefaultCharacterSet()));
            // TODO: Finish implementation (because this doesn't handle dictionaries, etc)
        } else {

        }
        return this;
    }

    /**
     * Copies the data at {@code rowIdx} to the end of the table
     * TODO: Implement
     *
     * @param rowIdx the index or row number of the row to copy
     */
    private void copyRow(int rowIdx) {

    }

    /**
     * Returns an existing dictionary for dictionary-encoded vectors if one exists in the provider.
     * Constructs and returns a Dictionary if the vector is encoded, but no Dictionary is available
     *
     * @param vector
     * @return
     */
    @Nullable
    private Dictionary dictionary(FieldVector vector) {
        Field field = table.getField(vector.getName());
        DictionaryEncoding dictionaryEncoding = field.getDictionary();
        if (dictionaryEncoding != null) {
            if (dictionaryProvider != null) {
                return dictionaryProvider.lookup(dictionaryEncoding.getId());
            }
            else {
                Dictionary dictionary = new Dictionary(vector, dictionaryEncoding);
                dictionaryProvider = new DictionaryProvider.MapDictionaryProvider(dictionary);
                return dictionary;
            }
        }
        return null;
    }
}
