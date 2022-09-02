package org.apache.arrow.table;

import org.apache.arrow.vector.*;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * MutableCursor is a positionable, immutable cursor backed by a {@link MutableTable}.
 * If a row in a table is marked as deleted, it is skipped when iterating.
 */
public class Cursor extends BaseCursor implements Iterator<Cursor> {

    /** Indicates whether the next non-deleted row has been determined yet */
    private boolean nextRowSet;

    /**
     * An iterator that returns every row in the table, deleted or not. The implemented next() and hasNext() methods
     * in Cursor wrap it with a filter to get only the non-deleted ones
     **/
    private final Iterator<Integer> iterator = intIterator();


    /**
     * Constructs a new BaseCursor backed by the given table.
     * @param table the table that this Cursor object represents
     */
    public Cursor(BaseTable table) {
        super(table);
    }

    /**
     * Constructs a newCursor backed by the given table
     * @param table     the table that this Cursor object represents
     * @param charset   the standard charset for decoding bytes into strings. Note: This can be overridden for
     *                  individual columns
     */
    public Cursor(BaseTable table, Charset charset) {
        super(table, charset);
    }

    @Override
    Cursor resetPosition() {
        return (Cursor) super.resetPosition();
    }

    /**
     * Moves this Cursor to the given 0-based row index
     * @return this Cursor for chaining
     **/
    public Cursor at(int rowNumber) {
        this.rowNumber = rowNumber;
        this.nextRowSet = false;
        return this;
    }

    /** Returns true if the value at columnName is null, and false otherwise */
    public boolean isNull(String columnName) {
        ValueVector vector = table.getVector(columnName);
        return vector.isNull(rowNumber);
    }

    /** Returns true if the value at columnName is null, and false otherwise */
    public boolean isNull(int columnIndex) {
        ValueVector vector = table.getVector(columnIndex);
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
     * Returns an int from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present in the MutableCursor and an IllegalArgumentException is thrown if it
     * has a different type
     */
    public int getUInt4(String columnName) {
        UInt4Vector vector = (UInt4Vector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns an int from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present and an IllegalArgumentException is thrown if it
     * has a different type
     */
    public int getUInt4(int columnIndex) {
        UInt4Vector vector = (UInt4Vector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns a short from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public short getSmallInt(String columnName) {
        SmallIntVector vector = (SmallIntVector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns a short from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public short getSmallInt(int columnIndex) {
        SmallIntVector vector = (SmallIntVector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns a char from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public char getUInt2(String columnName) {
        UInt2Vector vector = (UInt2Vector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns a char from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public char getUInt2(int columnIndex) {
        UInt2Vector vector = (UInt2Vector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns a byte from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public byte getTinyInt(String columnName) {
        TinyIntVector vector = (TinyIntVector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns a byte from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public byte getTinyInt(int columnIndex) {
        TinyIntVector vector = (TinyIntVector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns a byte from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public byte getUInt1(String columnName) {
        UInt1Vector vector = (UInt1Vector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns a byte from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public byte getUInt1(int columnIndex) {
        UInt1Vector vector = (UInt1Vector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public long getBigInt(String columnName) {
        BigIntVector vector = (BigIntVector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public long getBigInt(int columnIndex) {
        BigIntVector vector = (BigIntVector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public long getUInt8(String columnName) {
        UInt8Vector vector = (UInt8Vector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public long getUInt8(int columnIndex) {
        UInt8Vector vector = (UInt8Vector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns a float from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public float getFloat4(String columnName) {
        Float4Vector vector = (Float4Vector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public float getFloat4(int columnIndex) {
        Float4Vector vector = (Float4Vector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns a double from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public double getFloat8(String columnName) {
        Float8Vector vector = (Float8Vector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns a double from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public double getFloat8(int columnIndex) {
        Float8Vector vector = (Float8Vector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns an int from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public int getBit(String columnName) {
        BitVector vector = (BitVector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns an int from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public int getBit(int columnIndex) {
        BitVector vector = (BitVector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public long getTimeNano(String columnName) {
        TimeNanoVector vector = (TimeNanoVector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public long getTimeNano(int columnIndex) {
        TimeNanoVector vector = (TimeNanoVector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public long getTimeMicro(String columnName) {
        TimeMicroVector vector = (TimeMicroVector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public long getTimeMicro(int columnIndex) {
        TimeMicroVector vector = (TimeMicroVector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns an int from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public int getTimeMilli(String columnName) {
        TimeMilliVector vector = (TimeMilliVector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns an int from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public int getTimeMilli(int columnIndex) {
        TimeMilliVector vector = (TimeMilliVector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns an int from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public int getTimeSec(String columnName) {
        TimeSecVector vector = (TimeSecVector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns an int from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public int getTimeSec(int columnIndex) {
        TimeSecVector vector = (TimeSecVector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public long getTimeStampSec(String columnName) {
        TimeStampSecVector vector = (TimeStampSecVector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public long getTimeStampSec(int columnIndex) {
        TimeStampSecVector vector = (TimeStampSecVector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public long getTimeStampSecTZ(String columnName) {
        TimeStampSecTZVector vector = (TimeStampSecTZVector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public long getTimeStampSecTZ(int columnIndex) {
        TimeStampSecTZVector vector = (TimeStampSecTZVector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public long getTimeStampNano(String columnName) {
        TimeStampNanoVector vector = (TimeStampNanoVector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public long getTimeStampNano(int columnIndex) {
        TimeStampNanoVector vector = (TimeStampNanoVector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public long getTimeStampNanoTZ(String columnName) {
        TimeStampNanoTZVector vector = (TimeStampNanoTZVector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public long getTimeStampNanoTZ(int columnIndex) {
        TimeStampNanoTZVector vector = (TimeStampNanoTZVector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public long getTimeStampMilli(String columnName) {
        TimeStampMilliVector vector = (TimeStampMilliVector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public long getTimeStampMilli(int columnIndex) {
        TimeStampMilliVector vector = (TimeStampMilliVector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public long getTimeStampMilliTZ(String columnName) {
        TimeStampMilliTZVector vector = (TimeStampMilliTZVector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public long getTimeStampMilliTZ(int columnIndex) {
        TimeStampMilliTZVector vector = (TimeStampMilliTZVector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public long getTimeStampMicro(String columnName) {
        TimeStampMicroVector vector = (TimeStampMicroVector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public long getTimeStampMicro(int columnIndex) {
        TimeStampMicroVector vector = (TimeStampMicroVector) table.getVector(columnIndex);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column of the given name at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it
     * is present but has a different type
     */
    public long getTimeStampMicroTZ(String columnName) {
        TimeStampMicroTZVector vector = (TimeStampMicroTZVector) table.getVector(columnName);
        return vector.get(rowNumber);
    }

    /**
     * Returns a long from the column with the given index at the current row. An IllegalStateException is
     * thrown if the column is not present, and an IllegalArgumentException is thrown if it is present but
     * has a different type
     */
    public long getTimeStampMicroTZ(int columnIndex) {
        TimeStampMicroTZVector vector = (TimeStampMicroTZVector) table.getVector(columnIndex);
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
        return new String(vector.get(rowNumber), getDefaultCharacterSet());
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
        return new String(vector.get(rowNumber), getDefaultCharacterSet());
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
    public Cursor next() {
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
     * TODO: When used with an Table there would be no deleted rows, so this method could be used, but
     *   the deletion support is needed when it's needed as an immutable iterator for MutableTable.
     *
     * Returns new internal iterator that processes every row, deleted or not.
     * Users should use the wrapping next() and hasNext() methods rather than using this iterator directly,
     * unless you want to see any deleted rows.
     */
    private Iterator<Integer> intIterator() {
        return new Iterator<>() {

            @Override
            public boolean hasNext() {
                return rowNumber < table.getRowCount() - 1;
            }

            @Override
            public Integer next() {
                rowNumber++;
                return rowNumber;
            }
        };
    }

    public int getRowNumber() {
        return rowNumber;
    }

    private boolean rowIsDeleted(int rowNumber) {
        return table.isRowDeleted(rowNumber);
    }
}
