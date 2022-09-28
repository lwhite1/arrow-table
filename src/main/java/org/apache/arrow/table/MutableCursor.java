package org.apache.arrow.table;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * MutableCursor is a positionable, mutable cursor backed by a {@link MutableTable}.
 *
 * If a row in a table is marked as deleted, it is skipped when iterating.
 * TODO: Check for missing fixed-with type setters
 * TODO: Alternate setters(Object, (non-nullable) ValueHolder)
 */
public class MutableCursor extends Cursor {

    /** Lazy map of Fields to ValueHolders **/
    private final Map<Field, ValueHolder> holderMap = new HashMap<>();

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
     * Note: using setPosition() allows you to position the index at a deleted row,
     * while iterating skips the deleted rows.
     *
     * @return this Cursor for method chaining
     **/
    public MutableCursor setPosition(int rowNumber) {
        super.setPosition(rowNumber);
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
        v.setNull(getRowNumber());
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
        v.setNull(getRowNumber());
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
    public boolean isRowDeleted() {
        return table.isRowDeleted(getRowNumber());
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTinyInt(int columnIndex, byte value) {
        TinyIntVector v = (TinyIntVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTinyInt(String columnName, byte value) {
        TinyIntVector v = (TinyIntVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTinyInt(int columnIndex, NullableTinyIntHolder value) {
        TinyIntVector v = (TinyIntVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTinyInt(String columnName, NullableTinyIntHolder value) {
        TinyIntVector v = (TinyIntVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setSmallInt(int columnIndex, short value) {
        SmallIntVector v = (SmallIntVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setSmallInt(String columnName, short value) {
        SmallIntVector v = (SmallIntVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setSmallInt(int columnIndex, NullableSmallIntHolder value) {
        SmallIntVector v = (SmallIntVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setSmallInt(String columnName, NullableSmallIntHolder value) {
        SmallIntVector v = (SmallIntVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
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
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
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
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setInt(int columnIndex, NullableIntHolder value) {
        IntVector v = (IntVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setInt(String columnName, NullableIntHolder value) {
        IntVector v = (IntVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setBigInt(int columnIndex, long value) {
        BigIntVector v = (BigIntVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setBigInt(String columnName, long value) {
        BigIntVector v = (BigIntVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setBigInt(int columnIndex, NullableBigIntHolder value) {
        BigIntVector v = (BigIntVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setBigInt(String columnName, NullableBigIntHolder value) {
        BigIntVector v = (BigIntVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setUInt1(int columnIndex, byte value) {
        UInt1Vector v = (UInt1Vector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setUInt1(String columnName, byte value) {
        UInt1Vector v = (UInt1Vector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setUInt1(int columnIndex, NullableUInt1Holder value) {
        UInt1Vector v = (UInt1Vector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setUInt1(String columnName, NullableUInt1Holder value) {
        UInt1Vector v = (UInt1Vector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setUInt2(int columnIndex, short value) {
        UInt2Vector v = (UInt2Vector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setUInt2(String columnName, short value) {
        UInt2Vector v = (UInt2Vector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setUInt2(int columnIndex, NullableUInt2Holder value) {
        UInt2Vector v = (UInt2Vector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setUInt2(String columnName, NullableUInt2Holder value) {
        UInt2Vector v = (UInt2Vector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setUInt4(int columnIndex, int value) {
        UInt4Vector v = (UInt4Vector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setUInt4(String columnName, int value) {
        UInt4Vector v = (UInt4Vector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setUInt4(int columnIndex, NullableUInt4Holder value) {
        UInt4Vector v = (UInt4Vector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setUInt4(String columnName, NullableUInt4Holder value) {
        UInt4Vector v = (UInt4Vector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setUInt8(int columnIndex, long value) {
        UInt8Vector v = (UInt8Vector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setUInt8(String columnName, long value) {
        UInt8Vector v = (UInt8Vector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setUInt8(int columnIndex, NullableUInt8Holder value) {
        UInt8Vector v = (UInt8Vector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setUInt8(String columnName, NullableUInt8Holder value) {
        UInt8Vector v = (UInt8Vector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setFloat4(int columnIndex, float value) {
        Float4Vector v = (Float4Vector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setFloat4(String columnName, float value) {
        Float4Vector v = (Float4Vector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setFloat4(int columnIndex, NullableFloat4Holder value) {
        Float4Vector v = (Float4Vector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setFloat4(String columnName, NullableFloat4Holder value) {
        Float4Vector v = (Float4Vector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setFloat8(int columnIndex, double value) {
        Float8Vector v = (Float8Vector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setFloat8(String columnName, double value) {
        Float8Vector v = (Float8Vector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setFloat8(int columnIndex, NullableFloat8Holder value) {
        Float8Vector v = (Float8Vector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setFloat8(String columnName, NullableFloat8Holder value) {
        Float8Vector v = (Float8Vector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setDuration(int columnIndex, ArrowBuf value) {
        DurationVector v = (DurationVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setDuration(String columnName, ArrowBuf value) {
        DurationVector v = (DurationVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setDuration(int columnIndex, NullableDurationHolder value) {
        DurationVector v = (DurationVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setDuration(String columnName, NullableDurationHolder value) {
        DurationVector v = (DurationVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setIntervalDay(int columnIndex, ArrowBuf value) {
        IntervalDayVector v = (IntervalDayVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setIntervalDay(String columnName, ArrowBuf value) {
        IntervalDayVector v = (IntervalDayVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

  /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setIntervalDay(int columnIndex, NullableIntervalDayHolder value) {
        IntervalDayVector v = (IntervalDayVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setIntervalDay(String columnName, NullableIntervalDayHolder value) {
        IntervalDayVector v = (IntervalDayVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setIntervalYear(int columnIndex, int value) {
        IntervalYearVector v = (IntervalYearVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setIntervalYear(String columnName, int value) {
        IntervalYearVector v = (IntervalYearVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setIntervalYear(int columnIndex, NullableIntervalYearHolder value) {
        IntervalYearVector v = (IntervalYearVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setIntervalYear(String columnName, NullableIntervalYearHolder value) {
        IntervalYearVector v = (IntervalYearVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setIntervalMonthDayNano(int columnIndex, ArrowBuf value) {
        IntervalMonthDayNanoVector v = (IntervalMonthDayNanoVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setIntervalMonthDayNano(String columnName, ArrowBuf value) {
        IntervalMonthDayNanoVector v = (IntervalMonthDayNanoVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setIntervalMonthDayNano(int columnIndex, NullableIntervalMonthDayNanoHolder value) {
        IntervalMonthDayNanoVector v = (IntervalMonthDayNanoVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setIntervalMonthDayNano(String columnName, NullableIntervalMonthDayNanoHolder value) {
        IntervalMonthDayNanoVector v = (IntervalMonthDayNanoVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeNano(int columnIndex, long value) {
        TimeNanoVector v = (TimeNanoVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeNano(String columnName, long value) {
        TimeNanoVector v = (TimeNanoVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeNano(int columnIndex, NullableTimeNanoHolder value) {
        TimeNanoVector v = (TimeNanoVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeNano(String columnName, NullableTimeNanoHolder value) {
        TimeNanoVector v = (TimeNanoVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeMicro(int columnIndex, long value) {
        TimeMicroVector v = (TimeMicroVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeMicro(String columnName, long value) {
        TimeMicroVector v = (TimeMicroVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeMicro(int columnIndex, NullableTimeMicroHolder value) {
        TimeMicroVector v = (TimeMicroVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeMicro(String columnName, NullableTimeMicroHolder value) {
        TimeMicroVector v = (TimeMicroVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeMilli(int columnIndex, int value) {
        TimeMilliVector v = (TimeMilliVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeMilli(String columnName, int value) {
        TimeMilliVector v = (TimeMilliVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeMilli(int columnIndex, NullableTimeMilliHolder value) {
        TimeMilliVector v = (TimeMilliVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeMilli(String columnName, NullableTimeMilliHolder value) {
        TimeMilliVector v = (TimeMilliVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeSec(int columnIndex, int value) {
        TimeSecVector v = (TimeSecVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeSec(String columnName, int value) {
        TimeSecVector v = (TimeSecVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeSec(int columnIndex, NullableTimeSecHolder value) {
        TimeSecVector v = (TimeSecVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeSec(String columnName, NullableTimeSecHolder value) {
        TimeSecVector v = (TimeSecVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeStampNano(int columnIndex, long value) {
        TimeStampNanoVector v = (TimeStampNanoVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeStampNano(String columnName, long value) {
        TimeStampNanoVector v = (TimeStampNanoVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeStampNano(int columnIndex, NullableTimeStampNanoHolder value) {
        TimeStampNanoVector v = (TimeStampNanoVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeStampNano(String columnName, NullableTimeStampNanoHolder value) {
        TimeStampNanoVector v = (TimeStampNanoVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeStampMicro(int columnIndex, long value) {
        TimeStampMicroVector v = (TimeStampMicroVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeStampMicro(String columnName, long value) {
        TimeStampMicroVector v = (TimeStampMicroVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeStampMicro(int columnIndex, NullableTimeStampMicroHolder value) {
        TimeStampMicroVector v = (TimeStampMicroVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeStampMicro(String columnName, NullableTimeStampMicroHolder value) {
        TimeStampMicroVector v = (TimeStampMicroVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeStampMilli(int columnIndex, long value) {
        TimeStampMilliVector v = (TimeStampMilliVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeStampMilli(String columnName, long value) {
        TimeStampMilliVector v = (TimeStampMilliVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeStampMilli(int columnIndex, NullableTimeStampMilliHolder value) {
        TimeStampMilliVector v = (TimeStampMilliVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeStampMilli(String columnName, NullableTimeStampMilliHolder value) {
        TimeStampMilliVector v = (TimeStampMilliVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeStampSec(int columnIndex, long value) {
        TimeStampSecVector v = (TimeStampSecVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeStampSec(String columnName, long value) {
        TimeStampSecVector v = (TimeStampSecVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeStampSec(int columnIndex, NullableTimeStampSecHolder value) {
        TimeStampSecVector v = (TimeStampSecVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeStampSec(String columnName, NullableTimeStampSecHolder value) {
        TimeStampSecVector v = (TimeStampSecVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeStampNanoTZ(int columnIndex, long value) {
        TimeStampNanoTZVector v = (TimeStampNanoTZVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeStampNanoTZ(String columnName, long value) {
        TimeStampNanoTZVector v = (TimeStampNanoTZVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeStampNanoTZ(int columnIndex, NullableTimeStampNanoTZHolder value) {
        TimeStampNanoTZVector v = (TimeStampNanoTZVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeStampNanoTZ(String columnName, NullableTimeStampNanoTZHolder value) {
        TimeStampNanoTZVector v = (TimeStampNanoTZVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeStampMicroTZ(int columnIndex, long value) {
        TimeStampMicroTZVector v = (TimeStampMicroTZVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeStampMicroTZ(String columnName, long value) {
        TimeStampMicroTZVector v = (TimeStampMicroTZVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeStampMicroTZ(int columnIndex, NullableTimeStampMicroTZHolder value) {
        TimeStampMicroTZVector v = (TimeStampMicroTZVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeStampMicroTZ(String columnName, NullableTimeStampMicroTZHolder value) {
        TimeStampMicroTZVector v = (TimeStampMicroTZVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeStampMilliTZ(int columnIndex, long value) {
        TimeStampMilliTZVector v = (TimeStampMilliTZVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeStampMilliTZ(String columnName, long value) {
        TimeStampMilliTZVector v = (TimeStampMilliTZVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeStampMilliTZ(int columnIndex, NullableTimeStampMilliTZHolder value) {
        TimeStampMilliTZVector v = (TimeStampMilliTZVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeStampMilliTZ(String columnName, NullableTimeStampMilliTZHolder value) {
        TimeStampMilliTZVector v = (TimeStampMilliTZVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeStampSecTZ(int columnIndex, long value) {
        TimeStampSecTZVector v = (TimeStampSecTZVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeStampTZSec(String columnName, long value) {
        TimeStampSecTZVector v = (TimeStampSecTZVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setTimeStampSecTZ(int columnIndex, NullableTimeStampSecTZHolder value) {
        TimeStampSecTZVector v = (TimeStampSecTZVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setTimeStampTZSec(String columnName, NullableTimeStampSecTZHolder value) {
        TimeStampSecTZVector v = (TimeStampSecTZVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setDecimal(int columnIndex, ArrowBuf value) {
        DecimalVector v = (DecimalVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setDecimal(String columnName, ArrowBuf value) {
        DecimalVector v = (DecimalVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setDecimal(int columnIndex, NullableDecimalHolder value) {
        DecimalVector v = (DecimalVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setDecimal(String columnName, BigDecimal value) {
        DecimalVector v = (DecimalVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setDecimal(int columnIndex, BigDecimal value) {
        DecimalVector v = (DecimalVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setDecimal(String columnName, NullableDecimalHolder value) {
        DecimalVector v = (DecimalVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setDateMilli(int columnIndex, long value) {
        DateMilliVector v = (DateMilliVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setDateMilli(String columnName, long value) {
        DateMilliVector v = (DateMilliVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setDateMilli(int columnIndex, NullableDateMilliHolder value) {
        DateMilliVector v = (DateMilliVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setDateMilli(String columnName, NullableDateMilliHolder value) {
        DateMilliVector v = (DateMilliVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setFixedSizeBinary(int columnIndex, byte[] value) {
        FixedSizeBinaryVector v = (FixedSizeBinaryVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setFixedSizeBinary(String columnName, byte[] value) {
        FixedSizeBinaryVector v = (FixedSizeBinaryVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setFixedSizeBinary(int columnIndex, NullableFixedSizeBinaryHolder value) {
        FixedSizeBinaryVector v = (FixedSizeBinaryVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setFixedSizeBinary(String columnName, NullableFixedSizeBinaryHolder value) {
        FixedSizeBinaryVector v = (FixedSizeBinaryVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setBit(int columnIndex, int value) {
        BitVector v = (BitVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setBit(String columnName, int value) {
        BitVector v = (BitVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

   /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for method chaining
     */
    public MutableCursor setBit(int columnIndex, NullableBitHolder value) {
        BitVector v = (BitVector) table.getVector(columnIndex);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column with the given name at this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
     * IllegalArgumentException is thrown if it has a different type to that named in the method
     * signature
     *
     * @return this MutableCursor for chaining operations
     */
    public MutableCursor setBit(String columnName, NullableBitHolder value) {
        BitVector v = (BitVector) table.getVector(columnName);
        v.setSafe(getRowNumber(), value);
        return this;
    }

    /**
     * Sets the value of the column at the given index and this MutableCursor to the given value. An
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
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
     * IllegalStateException is thrown if the column is not present in the MutableCursor and an
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
        copyRow(rowIdx, nextRow);
        return nextRow;
    }

    /**
     * Copies the data at {@code rowIdx} to the end of the table
     *
     * @param fromIndex the index or row number of the row to copy
     * @param toIndex   the destination index (row number)
     */
    private void copyRow(int fromIndex, int toIndex) {

        for (FieldVector v: getTable().fieldVectors) {
            copyValue(v, fromIndex, toIndex);
        }
    }

    /**
     * Copies the value in vector V at position fromRow to position toRow
     * TODO: Handle nulls
     * TODO: Check for missing cases
     *
     * @param v         The FieldVector that is both the source and destination for the copy operation
     * @param fromRow   The row to copy from
     * @param toRow     The row to copy to
     */
    private void copyValue(FieldVector v, int fromRow, int toRow) {
        Types.MinorType type = v.getMinorType();
        switch (type) {
            case TINYINT:
                byte tinyIntValue = ((TinyIntVector) v).get(fromRow);
                ((TinyIntVector) v).setSafe(toRow, tinyIntValue);
                return;
            case SMALLINT:
                short smallIntValue = ((SmallIntVector) v).get(fromRow);
                ((SmallIntVector) v).setSafe(toRow, smallIntValue);
                return;
            case INT:
                int intValue = ((IntVector) v).get(fromRow);
                ((IntVector) v).setSafe(toRow, intValue);
                return;
            case BIGINT:
                long bigIntValue = ((BigIntVector) v).get(fromRow);
                ((BigIntVector) v).setSafe(toRow, bigIntValue);
                return;
            case UINT1:
                byte UInt1Value = ((UInt1Vector) v).get(fromRow);
                ((UInt1Vector) v).setSafe(toRow, UInt1Value);
                return;
            case UINT2:
                char UInt2Value = ((UInt2Vector) v).get(fromRow);
                ((UInt2Vector) v).setSafe(toRow, UInt2Value);
                return;
            case UINT4:
                int UInt4Value = ((UInt4Vector) v).get(fromRow);
                ((UInt4Vector) v).setSafe(toRow, UInt4Value);
                return;
            case UINT8:
                long UInt8Value = ((UInt8Vector) v).get(fromRow);
                ((UInt8Vector) v).setSafe(toRow, UInt8Value);
                return;
            case FLOAT4:
                float float4Value = ((Float4Vector) v).get(fromRow);
                ((Float4Vector) v).setSafe(toRow, float4Value);
                return;
            case FLOAT8:
                double float8Value = ((Float8Vector) v).get(fromRow);
                ((Float8Vector) v).setSafe(toRow, float8Value);
                return;
            case INTERVALYEAR:
                int intervalYearValue = ((IntervalYearVector) v).get(fromRow);
                ((IntervalYearVector) v).setSafe(toRow, intervalYearValue);
                return;
            case INTERVALDAY:
                ArrowBuf intervalDayValue = ((IntervalDayVector) v).get(fromRow);
                ((IntervalDayVector) v).setSafe(toRow, intervalDayValue);
                return;
            case INTERVALMONTHDAYNANO:
                ArrowBuf intervalMonthDayNanoValue = ((IntervalMonthDayNanoVector) v).get(fromRow);
                ((IntervalMonthDayNanoVector) v).setSafe(toRow, intervalMonthDayNanoValue);
                return;
            case TIMENANO:
                long timeNanoValue = ((TimeNanoVector) v).get(fromRow);
                ((TimeNanoVector) v).setSafe(toRow, timeNanoValue);
                return;
            case TIMEMICRO:
                long timeMicroValue = ((TimeMicroVector) v).get(fromRow);
                ((TimeMicroVector) v).setSafe(toRow, timeMicroValue);
                return;
            case TIMEMILLI:
                int timeMilliValue = ((TimeMilliVector) v).get(fromRow);
                ((TimeMilliVector) v).setSafe(toRow, timeMilliValue);
                return;
            case TIMESEC:
                int timeSecValue = ((TimeSecVector) v).get(fromRow);
                ((TimeSecVector) v).setSafe(toRow, timeSecValue);
                return;
            case DATEMILLI:
                long dateMilliValue = ((DateMilliVector) v).get(fromRow);
                ((DateMilliVector) v).setSafe(toRow, dateMilliValue);
                return;

            case TIMESTAMPNANO:
                long tsNanoValue = ((TimeStampNanoVector) v).get(fromRow);
                ((TimeStampNanoVector) v).setSafe(toRow, tsNanoValue);
                return;
            case TIMESTAMPMICRO:
                long tsMicroValue = ((TimeStampMicroVector) v).get(fromRow);
                ((TimeStampMicroVector) v).setSafe(toRow, tsMicroValue);
                return;
            case TIMESTAMPMILLI:
                long tsMilliValue = ((TimeStampMilliVector) v).get(fromRow);
                ((TimeStampMilliVector) v).setSafe(toRow, tsMilliValue);
                return;
            case TIMESTAMPSEC:
                long tsSecValue = ((TimeStampSecVector) v).get(fromRow);
                ((TimeStampSecVector) v).setSafe(toRow, tsSecValue);
                return;

            case TIMESTAMPNANOTZ:
                long tsNanoTZValue = ((TimeStampNanoTZVector) v).get(fromRow);
                ((TimeStampNanoTZVector) v).setSafe(toRow, tsNanoTZValue);
                return;
            case TIMESTAMPMICROTZ:
                long tsMicroTZValue = ((TimeStampMicroTZVector) v).get(fromRow);
                ((TimeStampMicroTZVector) v).setSafe(toRow, tsMicroTZValue);
                return;
            case TIMESTAMPMILLITZ:
                long tsMilliTZValue = ((TimeStampMilliTZVector) v).get(fromRow);
                ((TimeStampMilliTZVector) v).setSafe(toRow, tsMilliTZValue);
                return;
            case TIMESTAMPSECTZ:
                long tsSecTZValue = ((TimeStampSecTZVector) v).get(fromRow);
                ((TimeStampSecTZVector) v).setSafe(toRow, tsSecTZValue);
                return;

            case DECIMAL:
                ArrowBuf decimalValue = ((DecimalVector) v).get(fromRow);
                ((DecimalVector) v).setSafe(toRow, decimalValue);
                return;
            case FIXEDSIZEBINARY:
                byte[] fixedSizeBinaryValue = ((FixedSizeBinaryVector) v).get(fromRow);
                ((FixedSizeBinaryVector) v).setSafe(toRow, fixedSizeBinaryValue);
                return;
            case DURATION:
                ArrowBuf durationValue = ((DurationVector) v).get(fromRow);
                ((DurationVector) v).setSafe(toRow, durationValue);
                return;
            case VARBINARY:
                byte[] varBinaryValue = ((VarBinaryVector) v).get(fromRow);
                ((VarBinaryVector) v).setSafe(toRow, varBinaryValue);
                return;
            case LARGEVARBINARY:
                byte[] largeVarBinaryValue = ((LargeVarBinaryVector) v).get(fromRow);
                ((LargeVarBinaryVector) v).setSafe(toRow, largeVarBinaryValue);
                return;
            case BIT:
                int bitValue = ((BitVector) v).get(fromRow);
                ((BitVector) v).setSafe(toRow, bitValue);
                return;
            case VARCHAR:
                // TODO: Handle Dictionary Encoded case
                byte[] bytes = ((VarCharVector) v).get(fromRow);
                ((VarCharVector) v).set(toRow, bytes);
                return;
            default:
                throw new UnsupportedOperationException(buildErrorMessage("copy value", type));
        }
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
                Dictionary dictionary = dictionaryProvider.lookup(dictionaryEncoding.getId());
                if (dictionary == null) {
                    throw new IllegalStateException(
                            String.format("Field %s is dictionary encoded, but no Dictionary for that field is present in the DictionaryProvider for this table", field.getName()));

                }
            }
            else {
                throw new IllegalStateException(
                        String.format("Field %s is dictionary encoded, but no DictionaryProvider is present in the table.", field.getName()));
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

    void compact() {
        if (((MutableTable) table).deletedRowCount() == 0) {
            return;
        }
        int writePosition = 0;
        while(hasNext()) {
            next();
            while (isRowDeleted()) {
                next();
            }
            if (writePosition != rowNumber) {
                copyRow(rowNumber, writePosition);
                writePosition++;
            }
        }
        ((MutableTable) table).clearDeletedRows();
        ((MutableTable) table).setRowCount(writePosition);
    }

    /**
     */
    public IntHolder getIntHolder(String columnName) {
        return (IntHolder) getHolder(columnName);
    }

    /**
     * Returns the ValueHolder with the given name, or {@code null} if the name is not found. Names are case-sensitive.
     *
     * @param columnName   The name of the vector
     * @return the Vector with the given name, or null
     */
    ValueHolder getHolder(String columnName) {
        for (Map.Entry<Field, ValueHolder> entry: holderMap.entrySet()) {
            if (entry.getKey().getName().equals(columnName)) {
                return entry.getValue();
            }
        }
        IntVector v = (IntVector) table.getVector(columnName);
        IntHolder holder = new IntHolder();
        holderMap.put(v.getField(), holder);
        return holder;
    }

    /**
     * Sets the values in the map of ValueHolders to the columns with the associated name, in the current row
     *
     * An IllegalStateException is thrown if the column is not present in the MutableCursor
     * and an IllegalArgumentException is thrown if it is present, but has a type that is different from the ValueHolder.
     *
     * This method can be used to set multiple values in a single method invocation. The advantage of this over using
     * individual calls, is that some updates to variable width columns may cause the row to be deleted and
     * a new row added with the updated value. To do this repeatedly in a single row would cause unnecessary data
     * movement. For example:
     *<blockquote><pre>
     *     mutableTable.setVarChar("firstName", "John").setVarChar("lastName", "Smith");
     *</pre></blockquote>
     * <p>
     *     Might cause the updated row to be copied and marked for deletion twice.
     * <p>
     *     On the other hand, the following code would cause the row to be copied only once.
     * <p>
     * <blockquote><pre>
     *
     *     mutableTable.setAll("firstName", "John").setVarChar("lastName", "Smith");
     *  </pre></blockquote>
     * Note that there is no need to set all values this way unless desired
     *
     * @param valueMap  A map of vector names to value holders
     * @return          This cursor for chaining
     */
    public MutableCursor setAll(Map<String, ValueHolder> valueMap) {
        for (Map.Entry<String, ValueHolder> entry: valueMap.entrySet()) {
            FieldVector fv = table.getVector(entry.getKey());
            ValueHolder holder = entry.getValue();

            if (fv == null) {
               throw new IllegalStateException(String.format("Column %s is not present in the table.", entry.getKey()));
            }
            Types.MinorType type = fv.getMinorType();
            try {
                return setValue(fv, holder, type);
            } catch (ClassCastException cce) {
                throw new IllegalArgumentException(
                        String.format("Column %s has type %s, which does not match the provided ValueHolder",
                                entry.getKey(), type));
            }
        }
        return this;
    }

    /**
     * Sets the value in holder in the given fieldVector
     * @param fv
     * @param holder
     * @param type
     * @return
     */
    private MutableCursor setValue(FieldVector fv, ValueHolder holder, Types.MinorType type) {
        switch (type) {
            case TINYINT:
                ((TinyIntVector) fv).setSafe(getRowNumber(), (TinyIntHolder) holder);
                return this;
            case SMALLINT:
                ((SmallIntVector) fv).setSafe(getRowNumber(), (SmallIntHolder) holder);
                return this;
            case INT:
                ((IntVector) fv).setSafe(getRowNumber(), (IntHolder) holder);
                return this;
            case BIGINT:
                ((BigIntVector) fv).setSafe(getRowNumber(), (BigIntHolder) holder);
                return this;
            case UINT1:
                ((UInt1Vector) fv).setSafe(getRowNumber(), (UInt1Holder) holder);
                return this;
            case UINT2:
                ((UInt2Vector) fv).setSafe(getRowNumber(), (UInt2Holder) holder);
                return this;
            case UINT4:
                ((UInt4Vector) fv).setSafe(getRowNumber(), (UInt4Holder) holder);
                return this;
            case UINT8:
                ((UInt8Vector) fv).setSafe(getRowNumber(), (UInt8Holder) holder);
                return this;
            case FLOAT4:
                ((Float4Vector) fv).setSafe(getRowNumber(), (Float4Holder) holder);
                return this;
            case FLOAT8:
                ((Float8Vector) fv).setSafe(getRowNumber(), (Float8Holder) holder);
                return this;
            case DECIMAL:
                ((DecimalVector) fv).setSafe(getRowNumber(), (DecimalHolder) holder);
                return this;

                // TODO: Add remaining fixed-width types

            case VARCHAR:
                ((VarCharVector) fv).setSafe(getRowNumber(), (VarCharHolder) holder);
                return this;

                // TODO: Add remaining variable-width types

            default:
                throw new UnsupportedOperationException(buildErrorMessage("setAll", type));
        }
    }
}
