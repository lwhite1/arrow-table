package org.apache.arrow.table;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
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
        this(StreamSupport.stream(vectors.spliterator(), false).collect(Collectors.toList()));
    }

    /**
     * Constructs a new instance from vectors.
     */
    public static Table of(FieldVector... vectors) {
        return new Table(Arrays.stream(vectors).collect(Collectors.toList()));
    }

    /**
     * Constructs a new instance with the number of rows set to the value count of the first FieldVector
     *
     * All vectors must have the same value count. Although this is not checked, inconsistent counts may lead to
     * exceptions or other undefined behavior later.
     *
     * @param fieldVectors The data vectors (must be equal in size to <code>fields</code>.
     */
    public Table(List<FieldVector> fieldVectors) {
        this(fieldVectors, fieldVectors.size() == 0 ? 0 : fieldVectors.get(0).getValueCount());
    }

    /**
     * Constructs a new instance.
     *
     * @param fieldVectors The data vectors.
     * @param rowCount     The number of rows
     */
    public Table(List<FieldVector> fieldVectors, int rowCount) {
        super(fieldVectors, rowCount, null);
    }

    /**
     * Constructs a new instance.
     *
     * @param fieldVectors  The data vectors.
     * @param rowCount      The number of rows
     * @param provider      A dictionary provider. May be null if none of the vectors is dictionary encoded
     */
    public Table(List<FieldVector> fieldVectors, int rowCount, DictionaryProvider provider) {
        super(fieldVectors, rowCount, provider);
    }

/*
    */
/**
     * Constructs a new instance containing the children of parent but not the parent itself.
     *//*

    public Table(FieldVector parent) {
        this(parent.getField().getChildren(), parent.getChildrenFromFields(), parent.getValueCount());
    }
*/

    /**
     * Constructs a new instance containing the data from the argument. Vectors are shared
     * between the Table and VectorSchemaRoot. Direct modification of those vectors
     * is unsafe and should be avoided.
     *
     * @param vsr  The VectorSchemaRoot providing data for this Table
     */
    public Table(VectorSchemaRoot vsr) {
        this(vsr.getFieldVectors(), vsr.getRowCount());
        vsr.clear();
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
    public Table slice(int index, int length) {
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

        return new Table(sliceVectors);
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
     * Returns a MutableTable from the data in this table. Memory is transferred to the new table so this table
     * can no longer be used
     *
     * @return a new MutableTable
     */
    @Override
    public MutableTable toMutableTable() {
        MutableTable t = new MutableTable(
                fieldVectors.stream().map(v -> {
                    TransferPair transferPair = v.getTransferPair(v.getAllocator());
                    transferPair.transfer();
                    return (FieldVector) transferPair.getTo();
                }).collect(Collectors.toList())
        );
        clear();
        return t;
    }
}