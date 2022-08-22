package org.apache.arrow.table;

import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Abstract base class for Table with mutable and immutable concrete implementations
 */
public abstract class BaseTable implements AutoCloseable {

    protected final List<FieldVector> fieldVectors;

    protected final Map<Field, FieldVector> fieldVectorsMap = new LinkedHashMap<>();

    protected Schema schema;

    /**
     * The number of rows of data in the table; not necessarily the same as the table row capacity
     */
    protected int rowCount;

    /**
     *
     * @param schema
     * @param rowCount
     * @param fieldVectors
     */
    public BaseTable(Schema schema, int rowCount, List<FieldVector> fieldVectors) {
        this.schema = schema;
        this.rowCount = rowCount;
        this.fieldVectors = fieldVectors;
    }

    /**
     * Do an adaptive allocation of each vector for memory purposes. Sizes will be based on previously
     * defined initial allocation for each vector (and subsequent size learned).
     */
    public void allocateNew() {
        for (FieldVector v : fieldVectors) {
            v.allocateNew();
        }
        rowCount = 0;
    }

    /**
     * Returns a FieldReader for the vector with the given name
     * @param name  The name of a vector in this Table (case-sensitive)
     * @return      A FieldReader for the named FieldVector
     */
    public FieldReader getReader(String name) {
        for (Map.Entry<Field, FieldVector> entry : fieldVectorsMap.entrySet()) {
            if (entry.getKey().getName().equals(name)) {
                return entry.getValue().getReader();
            }
        }
        return null;
    }

    /**
     * Returns a FieldReader for the given field
     * @param field The field to be read
     * @return      A FieldReader for the given field
     */
    public FieldReader getReader(Field field) {
        return fieldVectorsMap.get(field).getReader();
    }

    public FieldReader getReader(int index) {
        Preconditions.checkArgument(index >= 0 && index < fieldVectors.size());
        return fieldVectors.get(index).getReader();
    }

    /**
     * Returns this table if it is Immutable; otherwise returns a new Immutable table from the data in this table
     */
    public abstract ImmutableTable toImmutableTable();

    /**
     * Returns this table if it is already Mutable; otherwise returns a new Mutable table from the data in this table
     */
    public abstract MutableTable toMutableTable();

    public Schema getSchema() {
        return schema;
    }

    /**
     * Returns a list of Field created by adding the given vector to the vectors in this Table.
     *
     * @param index  field index
     * @param vector vector to be added.
     * @return out List of FieldVectors with vector added
     */
    List<FieldVector> insertVector(int index, FieldVector vector) {
        Preconditions.checkNotNull(vector);
        Preconditions.checkArgument(index >= 0 && index < fieldVectors.size());
        List<FieldVector> newVectors = new ArrayList<>();
        for (int i = 0; i < fieldVectors.size(); i++) {
            if (i == index) {
                newVectors.add(vector);
            }
            newVectors.add(fieldVectors.get(i));
        }
        return newVectors;
    }

    /**
     * Returns a new List of FieldVectors created by removing the selected Vector from the list in this Table.
     *
     * @param index field index
     * @return out List of FieldVectos like the list in this table, but with the argument removed
     */
    public List<FieldVector> extractVector(int index) {
        Preconditions.checkArgument(index >= 0 && index < fieldVectors.size());
        List<FieldVector> newVectors = new ArrayList<>();
        for (int i = 0; i < fieldVectors.size(); i++) {
            if (i != index) {
                newVectors.add(fieldVectors.get(i));
            }
        }
        return newVectors;
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

    public long getRowCount() {
        return rowCount;
    }

    /**
     * Returns a new VectorSchemaRoot with the data and schema from this table
     * // TODO: Define the memory semantics of the copy and implement
     */
    public VectorSchemaRoot toVectorSchemaRoot() {
        return null;
    }

    /**
     * Returns the vector with the given name, or {@code null} if the name is not found. Names are case-sensitive.
     *
     *  @param columnName   The name of the vector
     * @return the Vector with the given name, or null
     */
    FieldVector getVector(String columnName) {
        for (Map.Entry<Field, FieldVector> entry: fieldVectorsMap.entrySet()) {
            if (entry.getKey().getName().equals(columnName)) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * Returns the vector at the given position
     * @param columnIndex   The 0-based position of the vector
     */
    FieldVector getVector(int columnIndex) {
        return fieldVectors.get(columnIndex);
    }

    public ImmutableCursor immutableCursor() {
        return new ImmutableCursor(this);
    }

    /**
     * Returns a tab separated value of vectors (based on their java object representation).
     * TODO: Consider moving to a separate object so code can be shared with VSR
     */
    public String contentToTSVString() {
        StringBuilder sb = new StringBuilder();
        List<Object> row = new ArrayList<>(schema.getFields().size());
        for (Field field : schema.getFields()) {
            row.add(field.getName());
        }
        printRow(sb, row);
        for (int i = 0; i < rowCount; i++) {
            row.clear();
            for (FieldVector v : fieldVectors) {
                row.add(v.getObject(i));
            }
            printRow(sb, row);
        }
        return sb.toString();
    }

    /**
     * Prints a single row without a header to the given StringBuilder
     * @param sb    the StringBuilder to write to
     * @param row   the row to write
     */
    private void printRow(StringBuilder sb, List<Object> row) {
        boolean first = true;
        for (Object v : row) {
            if (first) {
                first = false;
            } else {
                sb.append("\t");
            }
            sb.append(v);
        }
        sb.append("\n");
    }

    /**
     * Slice this table from desired index.
     * @param index start position of the slice
     * @return the sliced table
     */
    public BaseTable slice(int index) {
        return slice(index, this.rowCount - index);
    }

    /**
     * Slice this table at desired index and length.
     * @param index start position of the slice
     * @param length length of the slice
     * @return the sliced table
     */
    public BaseTable slice(int index, int length) {
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

        return new ImmutableTable(sliceVectors);
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
    public boolean isDeletedRow(int rowNumber) {
        return false;
    }
}
