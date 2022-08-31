package org.apache.arrow.table;

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
 * Table is an immutable tabular data structure.
 *
 * See {@link MutableTable} for a mutable version.
 * See {@link VectorSchemaRoot} for batch processing use cases
 */
public class Table extends BaseTable implements Iterable<Cursor> {

    /**
     * Constructs new instance containing each of the given vectors.
     */
    public Table(Iterable<FieldVector> vectors) {
        this(
                StreamSupport.stream(vectors.spliterator(), false).map(ValueVector::getField).collect(Collectors.toList()),
                StreamSupport.stream(vectors.spliterator(), false).collect(Collectors.toList())
        );
    }

    /**
     * Constructs a new instance containing the children of parent but not the parent itself.
     */
    public Table(FieldVector parent) {
        this(parent.getField().getChildren(), parent.getChildrenFromFields(), parent.getValueCount());
    }

    /**
     * Constructs a new instance containing the data from the argument. Vectors are shared
     * between the Table and VectorSchemaRoot. Direct modification of those vectors
     * is unsafe and should be avoided.
     *
     * @see Table#of(VectorSchemaRoot) for an alternative without data sharing
     *
     * @param vsr  The VectorSchemaRoot providing data for this Table
     */
    public Table(VectorSchemaRoot vsr) {
        this(vsr.getSchema(), vsr.getFieldVectors(), vsr.getRowCount());
    }

    /**
     * Constructs a new instance containing the data from the argument. The VectorSchemaRoot
     * is cleared in the process and its rowCount is set to 0. Memory used by the vectors
     * in the VectorSchemaRoot is transferred to the table.
     *
     * @see Table#Table(VectorSchemaRoot) for an alternative where data is shared with the
     * VectorSchemaRoot
     *
     * @param vsr  The VectorSchemaRoot providing data for this Table
     */
    public static Table of(VectorSchemaRoot vsr) {
        Table table = new Table(vsr.getSchema(),
                vsr.getFieldVectors().stream().map(v -> {
                    TransferPair transferPair = v.getTransferPair(v.getAllocator());
                    transferPair.transfer();
                    return (FieldVector) transferPair.getTo();
                }).collect(Collectors.toList()),
                vsr.getRowCount());
                vsr.clear();
        return table;
    }

    /**
     * Constructs a new instance with the number of rows set to the value count of the first FieldVector
     *
     * All vectors must have the same value count. Although this is not checked, inconsistent counts may lead to
     * exceptions or other undefined behavior later.
     *
     * @param fields       The types of each vector.
     * @param fieldVectors The data vectors (must be equal in size to <code>fields</code>.
     */
    public Table(List<Field> fields, List<FieldVector> fieldVectors) {
        this(new Schema(fields), fieldVectors, fieldVectors.size() == 0 ? 0 : fieldVectors.get(0).getValueCount());
    }

    /**
     * Constructs a new instance.
     *
     * @param fields       The types of each vector.
     * @param fieldVectors The data vectors (must be equal in size to <code>fields</code>.
     * @param rowCount     The number of rows contained.
     */
    public Table(List<Field> fields, List<FieldVector> fieldVectors, int rowCount) {
        this(new Schema(fields), fieldVectors, rowCount);
    }

    /**
     * Constructs a new instance.
     *
     * @param schema       The schema for the vectors.
     * @param fieldVectors The data vectors.
     * @param rowCount     The number of rows
     */
    public Table(Schema schema, List<FieldVector> fieldVectors, int rowCount) {
        super(schema, rowCount, fieldVectors);
    }

    /**
     * Constructs a new instance from vectors.
     */
    public static Table of(FieldVector... vectors) {
        return new Table(Arrays.stream(vectors).collect(Collectors.toList()));
    }

    /**
     * Returns a new Table created by adding the given vector to the vectors in this Table.
     *
     * @param index  field index
     * @param vector vector to be added.
     * @return out a new Table with vector added
     */
    public Table addVector(int index, FieldVector vector) {
        return new Table(insertVector(index, vector));
    }

    /**
     * Returns a new Table created by removing the selected Vector from this Table.
     *
     * @param index field index
     * @return out a new Table with vector removed
     */
    public Table removeVector(int index) {
        return new Table(extractVector(index));
    }

    /**
     * Slice this table from desired index. Memory is NOT transferred from the vectors in this table to new vectors in
     * the target table. This table is unchanged.
     *
     * @param index start position of the slice
     * @return the sliced table
     */
    public Table slice(int index) {
        return (Table) super.slice(index);
    }

    /**
     * Slice this table at desired index and length. Memory is NOT transferred from the vectors in this table to new
     * vectors in the target table. This table is unchanged.
     *
     * @param index start position of the slice
     * @param length length of the slice
     * @return the sliced table
     */
    public Table slice(int index, int length) {
        return (Table) super.slice(index, length);
    }

    /**
     * Returns a Cursor iterator for this Table
     */
    @Override
    public Iterator<Cursor> iterator() {

        return new Iterator<>() {

            private final Cursor row = new Cursor(Table.this);

            @Override
            public Cursor next() {
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
    public Table toImmutableTable() {
        return this;
    }

    /**
     * Returns a MutableTable from the data in this table
     * @return a new MutableTable
     */
    @Override
    public MutableTable toMutableTable() {
        return new MutableTable(
                fieldVectors.stream().map(v -> {
                    TransferPair transferPair = v.getTransferPair(v.getAllocator());
                    transferPair.transfer();
                    return (FieldVector) transferPair.getTo();
                }).collect(Collectors.toList())
        );
    }
}