package org.apache.arrow.table;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * ImmutableTable is an immutable, tabular data structure.
 *
 * See {@link MutableTable} for a mutable version.
 * See {@link VectorSchemaRoot} for batch processing use cases
 */
public class ImmutableTable extends BaseTable implements Iterable<ImmutableCursor> {

    /**
     * Constructs new instance containing each of the given vectors.
     */
    public ImmutableTable(Iterable<FieldVector> vectors) {
        this(
                StreamSupport.stream(vectors.spliterator(), false).map(ValueVector::getField).collect(Collectors.toList()),
                StreamSupport.stream(vectors.spliterator(), false).collect(Collectors.toList())
        );
    }

    /**
     * Constructs a new instance containing the children of parent but not the parent itself.
     */
    public ImmutableTable(FieldVector parent) {
        this(parent.getField().getChildren(), parent.getChildrenFromFields(), parent.getValueCount());
    }

    /**
     * Constructs a new instance containing the data from the argument.
     *
     * @param vsr  The VectorSchemaRoot providing data for this ImmutableTable
     */
    public ImmutableTable(VectorSchemaRoot vsr) {
        this(vsr.getSchema(), vsr.getFieldVectors(), vsr.getRowCount());
    }


    /**
     * Constructs a new instance.
     *
     * @param fields       The types of each vector.
     * @param fieldVectors The data vectors (must be equal in size to <code>fields</code>.
     */
    public ImmutableTable(List<Field> fields, List<FieldVector> fieldVectors) {
        this(new Schema(fields), fieldVectors, fieldVectors.size() == 0 ? 0 : fieldVectors.get(0).getValueCount());
    }

    /**
     * Constructs a new instance.
     *
     * @param fields       The types of each vector.
     * @param fieldVectors The data vectors (must be equal in size to <code>fields</code>.
     * @param rowCount     The number of rows contained.
     */
    public ImmutableTable(List<Field> fields, List<FieldVector> fieldVectors, int rowCount) {
        this(new Schema(fields), fieldVectors, rowCount);
    }

    /**
     * Constructs a new instance.
     *
     * @param schema       The schema for the vectors.
     * @param fieldVectors The data vectors.
     * @param rowCount     The number of rows
     */
    public ImmutableTable(Schema schema, List<FieldVector> fieldVectors, int rowCount) {
        super(schema, rowCount, fieldVectors);
        if (schema.getFields().size() != fieldVectors.size()) {
            throw new IllegalArgumentException("Fields must match field vectors. Found " +
                    fieldVectors.size() + " vectors and " + schema.getFields().size() + " fields");
        }
        for (int i = 0; i < schema.getFields().size(); ++i) {
            Field field = schema.getFields().get(i);
            FieldVector vector = fieldVectors.get(i);
            fieldVectorsMap.put(field, vector);
        }
    }

    /**
     * Creates a new set of empty vectors corresponding to the given schema.
     */
    public static ImmutableTable create(Schema schema, BufferAllocator allocator) {
        List<FieldVector> fieldVectors = new ArrayList<>();
        for (Field field : schema.getFields()) {
            FieldVector vector = field.createVector(allocator);
            fieldVectors.add(vector);
        }
        if (fieldVectors.size() != schema.getFields().size()) {
            throw new IllegalArgumentException("The root vector did not create the right number of children. found " +
                    fieldVectors.size() + " expected " + schema.getFields().size());
        }
        return new ImmutableTable(schema, fieldVectors, 0);
    }

    /**
     * Constructs a new instance from vectors.
     */
    public static ImmutableTable of(FieldVector... vectors) {
        return new ImmutableTable(Arrays.stream(vectors).collect(Collectors.toList()));
    }

    /**
     * Sets the rowCount for this MutableTable, and the valueCount for each of its vectors to the argument
     * @param rowCount  the number of rows in the table and in each vector
     */
    private void setRowCount(int rowCount) {
        this.rowCount = rowCount;
        for (FieldVector v : fieldVectors) {
            v.setValueCount(rowCount);
        }
    }

    /**
     * Returns a new Table created by adding the given vector to the vectors in this Table.
     *
     * @param index  field index
     * @param vector vector to be added.
     * @return out a new Table with vector added
     */
    public ImmutableTable addVector(int index, FieldVector vector) {
        return new ImmutableTable(insertVector(index, vector));
    }

    /**
     * Returns a new Table created by removing the selected Vector from this Table.
     *
     * @param index field index
     * @return out a new Table with vector removed
     */
    public ImmutableTable removeVector(int index) {
        return new ImmutableTable(extractVector(index));
    }

    /**
     * Returns an ImmutableCursor iterator for this Table
     */
    @Override
    public Iterator<ImmutableCursor> iterator() {

        return new Iterator<>() {

            private final ImmutableCursor row = new ImmutableCursor(ImmutableTable.this);

            @Override
            public ImmutableCursor next() {
                row.next();
                return row;
            }

            @Override
            public boolean hasNext() {
                return row.hasNext();
            }
        };
    }

    @Override
    public ImmutableTable toImmutableTable() {
        return this;
    }

    /**
     * Returns a MutableTable from the data in this table
     * // TODO: Implement
     * @return a new MutableTable
     */
    @Override
    public MutableTable toMutableTable() {
        return null;
    }
}