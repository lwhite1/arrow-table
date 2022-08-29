package org.apache.arrow.table;

import org.apache.arrow.table.util.BasicTypeHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

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
     * A map of vectors to valueHolders
     */
    private final Map<FieldVector, ValueHolder> holderMap = new HashMap<>();

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
     * Note: using at() allows you to position the index at a deleted row, while iterating skips the deleted rows.
     * TODO: consider whether this is preferable to
     *      (a) providing an iterator that includes deleted rows, or
     *      (b) providing something like atAny() that includes deleted rows, while the standard at would skip
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
    public MutableCursor deleteCurrentRow() {
        getTable().markRowDeleted(getRowNumber());
        return this;
    }

    /**
     * Returns true if the current row is marked as deleted and false otherwise
     */
    public boolean isDeletedRow() {
        return table.isDeletedRow(getRowNumber());
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
        v.setSafe(getRowNumber(), value);
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
        v.setSafe(getRowNumber(), value);
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
        Dictionary dictionary = dictionary(v);
        if (dictionary != null) {
            v.set(getRowNumber(), value.getBytes(getDefaultCharacterSet()));
            // TODO: Finish dictionary implementation
        } else {
            // There is no dictionary encoding here, so copy the row and mark the current row for deletion
            deleteCurrentRow();
            int newRow = copyRow(getRowNumber());
            v.set(newRow, value.getBytes(getDefaultCharacterSet()));
        }
        return this;    }

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
            // TODO: Finish dictionary implementation
        } else {
            // There is no dictionary encoding here, so copy the row and mark the current row for deletion
            deleteCurrentRow();
            int newRow = copyRow(getRowNumber());
            v.setSafe(newRow, value.getBytes(getDefaultCharacterSet()));
        }
        return this;
    }

    /**
     * Copies the data at {@code rowIdx} to the end of the table
     *
     * @param rowIdx    the index or row number of the row to copy
     * @return          the index of the new row
     */
    private int copyRow(int rowIdx) {

        int nextRow = table.rowCount++;
        for (FieldVector v: getTable().fieldVectors) {
            copyValue(v, rowIdx, nextRow);
        }
        return nextRow;
    }

    private void copyValue(FieldVector v, int fromRow, int toRow) {
        Types.MinorType type = v.getMinorType();
        switch (type) {
/*
            case TINYINT:
                return new NullableTinyIntHolder();
            case UINT1:
                return new NullableUInt1Holder();
            case UINT2:
                return new NullableUInt2Holder();
            case SMALLINT:
                return new NullableSmallIntHolder();
*/
            case INT:
                int intValue = ((IntVector) v).get(fromRow);
                ((IntVector) v).setSafe(toRow, intValue);
                return;
/*
            case UINT4:
                return new NullableUInt4Holder();

            case FLOAT4:
                return new NullableFloat4Holder();
            case INTERVALYEAR:
                return new NullableIntervalYearHolder();
            case TIMEMILLI:
                return new NullableTimeMilliHolder();
            case BIGINT:
                return new NullableBigIntHolder();
            case UINT8:
                return new NullableUInt8Holder();
            case FLOAT8:
                return new NullableFloat8Holder();
            case DATEMILLI:
                return new NullableDateMilliHolder();
            case TIMESTAMPMILLI:
                return new NullableTimeStampMilliHolder();
            case INTERVALDAY:
                return new NullableIntervalDayHolder();
            case DECIMAL:
                return new NullableDecimalHolder();
            case FIXEDSIZEBINARY:
                return new NullableFixedSizeBinaryHolder();
            case VARBINARY:
                return new NullableVarBinaryHolder();

 */
            case VARCHAR:
                byte[] bytes = ((VarCharVector) v).get(fromRow);
                ((VarCharVector) v).set(toRow, bytes);
                return;
 /*
            case BIT:
                return new NullableBitHolder();
  */
            default:
                throw new UnsupportedOperationException(buildErrorMessage("copy value", type));
        }
    }


    /**
     * Creates a new ValueHolder for the given vector
     * @param vector    The vector that needs a ValueHolder
     * @return          The ValueHolder
     */
    ValueHolder createHolder(FieldVector vector) {
        ValueHolder holder = BasicTypeHelper.createValueHolder(vector.getMinorType());
        // TODO: set the holder's vector to the argument
        return holder;
    }

    /**
     * Returns the ValueHolder for the given vector. If no ValueHolder is available, a new one is constructed
     * and placed in a map for future use before returning.
     *
     * @param vector    The FieldVector in need of a ValueHolder
     * @return          The ValueHolder for the given vector
     */
    ValueHolder getHolder(FieldVector vector) {
        ValueHolder holder = holderMap.get(vector);
        if (holder == null) {
            holder = createHolder(vector);
            holderMap.put(vector, holder);
        }
        return holder;
    }

    /**
     * Sets a dictionary provider for use with this cursor
     * @param dictionaryProvider    The provider to use
     */
    public void setDictionaryProvider(DictionaryProvider dictionaryProvider) {
        this.dictionaryProvider = dictionaryProvider;
    }

    /**
     * Returns an existing dictionary for dictionary-encoded vectors if one exists in the provider.
     * Constructs and returns a Dictionary if the vector is encoded, but no Dictionary is available
     *
     * @param vector    A dictionary-encoded vector
     * @return          A dictionary for the provided vector
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

    protected static String buildErrorMessage(final String operation, final Types.MinorType type) {
        return String.format("Unable to %s for minor type [%s]", operation, type);
    }

    @Override
    MutableCursor resetPosition() {
        return (MutableCursor) super.resetPosition();
    }
}
