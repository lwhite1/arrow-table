package org.apache.arrow.table;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * MutableTable is a mutable tabular data structure for static or interactive use.
 * See {@link org.apache.arrow.vector.VectorSchemaRoot} for batch processing use cases
 */
public class MutableTable extends BaseTable implements AutoCloseable, Iterable<MutableCursor> {

    /**
     * Indexes of any rows that have been marked for deletion
     * TODO: This is a prototype implementation. Replace with bitmap vector
     */
    private final Set<Integer> deletedRows = new HashSet<>();

    /**
     * Constructs new instance containing each of the given vectors.
     */
    public MutableTable(Iterable<FieldVector> vectors) {
        this(
                StreamSupport.stream(vectors.spliterator(), false).map(ValueVector::getField).collect(Collectors.toList()),
                StreamSupport.stream(vectors.spliterator(), false).collect(Collectors.toList())
        );
    }

    /**
     * Constructs a new instance containing the children of parent but not the parent itself.
     */
    public MutableTable(FieldVector parent) {
        this(parent.getField().getChildren(), parent.getChildrenFromFields(), parent.getValueCount());
    }

    /**
     * Constructs a new instance containing the data from the argument.
     *
     * @param vsr  The VectorSchemaRoot providing data for this MutableTable
     */
    public MutableTable(VectorSchemaRoot vsr) {
        this(vsr.getSchema(), vsr.getFieldVectors(), vsr.getRowCount());
    }


    /**
     * Constructs a new instance.
     *
     * @param fields       The types of each vector.
     * @param fieldVectors The data vectors (must be equal in size to <code>fields</code>.
     */
    public MutableTable(List<Field> fields, List<FieldVector> fieldVectors) {
        this(new Schema(fields), fieldVectors, fieldVectors.size() == 0 ? 0 : fieldVectors.get(0).getValueCount());
    }

    /**
     * Constructs a new instance.
     *
     * @param fields       The types of each vector.
     * @param fieldVectors The data vectors (must be equal in size to <code>fields</code>.
     * @param rowCount     The number of rows contained.
     */
    public MutableTable(List<Field> fields, List<FieldVector> fieldVectors, int rowCount) {
        this(new org.apache.arrow.vector.types.pojo.Schema(fields), fieldVectors, rowCount);
    }

    /**
     * Constructs a new instance.
     *
     * @param schema       The schema for the vectors.
     * @param fieldVectors The data vectors.
     * @param rowCount     The number of rows
     */
    public MutableTable(Schema schema, List<FieldVector> fieldVectors, int rowCount) {
        super(schema, rowCount, fieldVectors);
    }

    /**
     * Creates a new set of empty vectors corresponding to the given schema.
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
        return new MutableTable(schema, fieldVectors, 0);
    }

    /**
     * Constructs a new instance from vectors.
     */
    public static MutableTable of(FieldVector... vectors) {
        return new MutableTable(Arrays.stream(vectors).collect(Collectors.toList()));
    }

    /**
     * Returns a copy of this table, with all deleted rows removed
     *
     * @return a new table with no empty rows among the data
     */
    public MutableTable compact() {
        MutableTable compacted = null;
        // TODO: Implement me

        deletedRows.clear();
        return this;
    }

    /**
     * Release all the memory for each vector held in this table. This DOES NOT remove vectors from the container.
     */
    public void clear() {
        for (FieldVector v : fieldVectors) {
            v.clear();
        }
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


    /**
     * Returns a ImmutableTable from the data in this table
     * // TODO: Implement
     * @return a new ImmutableTable
     */
    @Override
    public ImmutableTable toImmutableTable() {
        return null;
    }

    @Override
    public MutableTable toMutableTable() {
        return this;
    }

    @Override
    public void close() {
        try {
            AutoCloseables.close(fieldVectors);
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            // should never happen since FieldVector.close() doesn't throw IOException
            throw new RuntimeException(ex);
        }
    }

    /**
     * Sets the rowCount for this MutableTable, and the valueCount for each of its vectors to the argument
     * @param rowCount  the number of rows in the table and in each vector
     */
    public void setRowCount(int rowCount) {
        super.rowCount = rowCount;
        for (FieldVector v : fieldVectors) {
            v.setValueCount(rowCount);
        }
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

    public MutableCursor mutableCursor() {
        return new MutableCursor(this);
    }

    /**
     * Returns a cursor with only 'get' operations. Use a MutableCursor if you need to update the data
     * @return  a new ImmutableCursor for this table
     */
    public ImmutableCursor immutableCursor() {
        return new ImmutableCursor(this);
    }

    /**
     * Slice this table from desired index.
     * @param index start position of the slice
     * @return the sliced table
     */
    @Override
    public MutableTable slice(int index) {
        return slice(index, this.rowCount - index);
    }

    /**
     * Slice this table at desired index and length.
     * @param index start position of the slice
     * @param length length of the slice
     * @return the sliced table
     */
    @Override
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
    public boolean isDeletedRow(int rowNumber) {
        if (rowNumber >= rowCount) {
            return true;
        }
        return deletedRows.contains(rowNumber);
    }
}