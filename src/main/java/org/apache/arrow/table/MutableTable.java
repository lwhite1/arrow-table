package org.apache.arrow.table;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * MutableTable is a mutable tabular data structure for static or interactive use.
 * See {@link org.apache.arrow.vector.VectorSchemaRoot} for batch processing use cases
 *
 * TODO:
 *      Add a concatenate method that takes Table arguments
 *      Add a method that concatenates a VSR to an existing Table
 *      Extend the concatenate method here to immutable Table
 *      Consider adjusting the memory handling of the concatenate methods
 *      Consider removing the constructors that share memory with externally constructed vectors
 */
public class MutableTable extends BaseTable implements AutoCloseable, Iterable<MutableCursor> {

    /**
     * Indexes of any rows that have been marked for deletion
     * TODO: This is a prototype implementation. Replace with bitmap vector
     */
    private final Set<Integer> deletedRows = new HashSet<>();

    private DictionaryProvider dictionaryProvider;

    /**
     * Constructs new instance containing each of the given vectors.
     */
    public MutableTable(Iterable<FieldVector> vectors) {
        this(StreamSupport.stream(vectors.spliterator(), false).collect(Collectors.toList()));
    }

    /**
     * Constructs a new instance from vectors.
     */
    public static MutableTable of(FieldVector... vectors) {
        return new MutableTable(Arrays.stream(vectors).collect(Collectors.toList()));
    }

    /**
     * Constructs new instance containing each of the given vectors.
     */
    public MutableTable(List<FieldVector> fieldVectors) {
        this(fieldVectors, fieldVectors.size() == 0 ? 0 : fieldVectors.get(0).getValueCount());
    }

/*
    */
/**
     * Constructs a new instance containing the children of parent but not the parent itself.
     *//*

    public MutableTable(FieldVector parent) {
        this(parent.getField().getChildren(), parent.getChildrenFromFields(), parent.getValueCount());
    }
*/

    /**
     * Constructs a new instance containing the data from the argument. The VectorSchemaRoot
     * is cleared in the process and its rowCount is set to 0. Memory used by the vectors
     * in the VectorSchemaRoot is transferred to the table.
     *
     * @param vsr  The VectorSchemaRoot providing data for this MutableTable
     */
    public MutableTable(VectorSchemaRoot vsr) {
        this(vsr.getFieldVectors(), vsr.getRowCount());
        vsr.clear();
    }

    /**
     * Constructs a new instance.
     *
     * @param fieldVectors The data vectors.
     * @param rowCount     The number of rows
     */
    public MutableTable(List<FieldVector> fieldVectors, int rowCount) {
        super(fieldVectors, rowCount);
    }

    /**
     * Constructs a new instance.
     *
     * @param fieldVectors          The data vectors.
     * @param rowCount              The number of rows
     * @param dictionaryProvider    The dictionary provider containing the dictionaries for any encoded column
     */
    public MutableTable(List<FieldVector> fieldVectors, int rowCount, DictionaryProvider dictionaryProvider) {
        super(fieldVectors, rowCount);
        this.dictionaryProvider = dictionaryProvider;
    }

    /**
     * Creates a table on a new set of empty vectors corresponding to the given schema.
     */
    public static MutableTable create(Schema schema, BufferAllocator allocator) {
        List<FieldVector> fieldVectors = new ArrayList<>();
        for (Field field : schema.getFields()) {
            FieldVector vector = field.createVector(allocator);
            fieldVectors.add(vector);
        }
        if (fieldVectors.size() != schema.getFields().size()) {
            throw new IllegalArgumentException("The root vector did not create the right number of children. found " +
                    fieldVectors.size() + " expected " + schema.getFields().size());
        }
        return new MutableTable(fieldVectors, 0);
    }

    /**
     * Returns a new Table made by concatenating a number of VectorSchemaRoots with the same schema
     *
     * @param roots the VectorSchemaRoots to concatenate
     */
    public static MutableTable concatenate(BufferAllocator allocator, List<VectorSchemaRoot> roots) {
        assert roots.size() > 0;
        Schema firstSchema = roots.get(0).getSchema();
        int totalRowCount = 0;
        for (VectorSchemaRoot root : roots) {
            if (!root.getSchema().equals(firstSchema))
                throw new IllegalArgumentException("All tables must have the same schema");
            totalRowCount += root.getRowCount();
        }

        final int finalTotalRowCount = totalRowCount;
        FieldVector[] newVectors = roots.get(0).getFieldVectors().stream()
                .map(vec -> {
                    FieldVector newVector = (FieldVector) vec.getTransferPair(allocator).getTo();
                    newVector.setInitialCapacity(finalTotalRowCount);
                    newVector.allocateNew();
                    newVector.setValueCount(finalTotalRowCount);
                    return newVector;
                })
                .toArray(FieldVector[]::new);

        int offset = 0;
        for (VectorSchemaRoot root : roots) {
            int rowCount = root.getRowCount();
            for (int i = 0; i < newVectors.length; i++) {
                FieldVector oldVector = root.getVector(i);
                retryCopyFrom(newVectors[i], oldVector, 0, rowCount, offset);
            }
            offset += rowCount;
        }
        return MutableTable.of(newVectors);
    }

    /**
     * Instead of using copyFromSafe, which checks for memory on each write,
     * this tries to copy over the entire vector and retry if it fails.
     *
     * @param newVector the vector to copy to
     * @param oldVector the vector to copy from
     * @param oldStart the starting index in the old vector
     * @param oldEnd the ending index in the old vector
     * @param newStart the starting index in the new vector
     */
    private static void retryCopyFrom(ValueVector newVector, ValueVector oldVector, int oldStart, int oldEnd, int newStart) {
        while (true) {
            try {
                for (int i = oldStart; i < oldEnd; i++) {
                    newVector.copyFrom(i, i - oldStart + newStart, oldVector);
                }
                break;
            }
            catch (IndexOutOfBoundsException err) {
                newVector.reAlloc();
            }
        }
    }

    /**
     * Do an adaptive allocation of each vector for memory purposes. Sizes will be based on previously
     * defined initial allocation for each vector (and subsequent size learned).
     */
    void allocateNew() {
        for (FieldVector v : fieldVectors) {
            v.allocateNew();
        }
        rowCount = 0;
    }

    /**
     * Returns this table with no deleted rows
     *
     * @return this table with any deleted rows removed
     */
    public MutableTable compact() {
        mutableCursor().compact();
        return this;
    }

    void clearDeletedRows() {
        this.deletedRows.clear();
    }

    /**
     * Release all the memory for each vector held in this table. This DOES NOT remove vectors from the container.
     */
    public void clear() {
        for (FieldVector v : fieldVectors) {
            v.clear();
        }
        clearDeletedRows();
        rowCount = 0;
    }

    /**
     * Returns a new Table created by adding the given vector to the vectors in this Table.
     *
     * @param index  field index
     * @param vector vector to be added.
     * @return out a copy of this Table with vector added
     */
    public MutableTable addVector(int index, FieldVector vector) {
        return new MutableTable(insertVector(index, vector));
    }

    /**
     * Returns a new Table created by removing the selected Vector from this Table.
     *
     * @param index field index
     * @return out a copy of this Table with vector removed
     */
    public MutableTable removeVector(int index) {
        return new MutableTable(extractVector(index));
    }

    public int deletedRowCount() {
        return this.deletedRows.size();
    }

    /**
     * Returns a Table from the data in this table. Memory is transferred to the new table so this mutable table
     * can no longer be used
     *
     * @return a new Table containing the data in this table
     */
    @Override
    public Table toImmutableTable() {
        Table t = new Table(
                fieldVectors.stream().map(v -> {
                    TransferPair transferPair = v.getTransferPair(v.getAllocator());
                    transferPair.transfer();
                    return (FieldVector) transferPair.getTo();
                }).collect(Collectors.toList())
        );
        clear();
        return t;
    }

    @Override
    public MutableTable toMutableTable() {
        return this;
    }

    /**
     * Sets the rowCount for this MutableTable, and the valueCount for each of its vectors to the argument
     * @param rowCount  the number of rows in the table and in each vector
     */
    public void setRowCount(int rowCount) {
        super.rowCount = rowCount;
/*
        TODO: Double check that this isn't wanted
        TODO: Should this be public?
        for (FieldVector v : fieldVectors) {
            v.setValueCount(rowCount);
        }
*/
    }

    /**
     * Marks the row at the given index as deleted
     *
     * @param rowNumber The 0-based index of the row to be deleted
     */
    public void markRowDeleted(int rowNumber) {
        if (rowNumber >= rowCount) {
            return;
        }
        deletedRows.add(rowNumber);
    }

    /**
     * Returns a MutableCursor iterator for this MutableTable
     */
    @Override
    public Iterator<MutableCursor> iterator() {

        return new Iterator<>() {

            private final MutableCursor row = new MutableCursor(MutableTable.this);

            @Override
            public MutableCursor next() {
                row.next();
                return row;
            }

            @Override
            public boolean hasNext() {
                return row.hasNext();
            }
        };
    }

    /**
     * Returns a Cursor allowing the user to read and modify the values in this table
     *
     * @return a MutableCursor providing access to this table
     */
    public MutableCursor mutableCursor() {
        return new MutableCursor(this);
    }

    /**
     * Returns a Cursor allowing the user to read and modify the values in this table. Character reads and writes
     * are performed using the given charset, unless a method with a Charset argument is used for a particular vector.
     *
     * @param defaultCharset    A charset to use as the default for reading any varchar data
     * @return                  a MutableCursor providing access to this table
     */
    public MutableCursor mutableCursor(Charset defaultCharset) {
        return new MutableCursor(this, defaultCharset);
    }

    /**
     * Slice this table from desired index. Memory is NOT transferred from the vectors in this table to new vectors in
     * the target table. This table is unchanged.
     *
     * @param index start position of the slice
     * @return the sliced table
     */
    public MutableTable slice(int index) {
        return slice(index, this.rowCount - index);
    }

    /**
     * Slice this table at desired index and length. Memory is NOT transferred from the vectors in this table to new
     * vectors in the target table. This table is unchanged.
     *
     * @param index start position of the slice
     * @param length length of the slice
     * @return the sliced table
     */
    public MutableTable slice(int index, int length) {
        Preconditions.checkArgument(index >= 0, "expecting non-negative index");
        Preconditions.checkArgument(length >= 0, "expecting non-negative length");
        Preconditions.checkArgument(index + length <= rowCount,
                "index + length should <= rowCount");

        if (index == 0 && length == rowCount) {
            return this;
        }

        List<FieldVector> sliceVectors = fieldVectors.stream().map(v -> {
            TransferPair transferPair = v.getTransferPair(v.getAllocator());
            transferPair.splitAndTransfer(index, length);
            return (FieldVector) transferPair.getTo();
        }).collect(Collectors.toList());

        return new MutableTable(sliceVectors);
    }

    /**
     * Returns true if the row at the given index has been deleted and false otherwise
     *
     * If the index is larger than the number of rows, the method returns true.
     * TODO: Consider renaming to a test that includes the notion of within the valid range
     *
     * @param rowNumber The 0-based index of the possibly deleted row
     * @return  true if the row at the index was deleted; false otherwise
     */
    @Override
    public boolean isRowDeleted(int rowNumber) {
        if (rowNumber >= rowCount) {
            return true;
        }
        return deletedRows.contains(rowNumber);
    }

    /**
     * Returns the dictionaryProvider associated with this table, if any
     * @return a DictionaryProvider or null
     */
    @Nullable
    public DictionaryProvider getDictionaryProvider() {
        return dictionaryProvider;
    }
}