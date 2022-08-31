package org.apache.arrow.table;

import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.nio.charset.Charset;
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
     * Constructs new instance with the given rowCount, and containing the schema and each of the given vectors.
     * @param schema        the schema for this Table
     * @param rowCount      the number of rows in the table
     * @param fieldVectors  the FieldVectors containing the table's data
     */
    public BaseTable(Schema schema, int rowCount, List<FieldVector> fieldVectors) {
        if (schema.getFields().size() != fieldVectors.size()) {
            throw new IllegalArgumentException("Fields must match field vectors. Found " +
                    fieldVectors.size() + " vectors and " + schema.getFields().size() + " fields");
        }
        this.schema = schema;
        this.rowCount = rowCount;
        this.fieldVectors = fieldVectors;
        for (int i = 0; i < schema.getFields().size(); ++i) {
            Field field = schema.getFields().get(i);
            FieldVector vector = fieldVectors.get(i);
            fieldVectorsMap.put(field, vector);
            vector.setValueCount(rowCount);
        }
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
    public abstract Table toImmutableTable();

    /**
     * Returns this table if it is already Mutable; otherwise returns a new Mutable table from the data in this table
     */
    public abstract MutableTable toMutableTable();

    public Schema getSchema() {
        return schema;
    }

    /**
     * Returns the Field with the given name if one exists in this table
     * @param fieldName the name of the field to return
     * @return          a field with the given name if one is present
     * @throws IllegalArgumentException – if the field was not found
     */
    public Field getField(String fieldName) {
        return getSchema().findField(fieldName);
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
        Preconditions.checkArgument(index >= 0 && index <= fieldVectors.size());
        List<FieldVector> newVectors = new ArrayList<>();
        if (index == fieldVectors.size()) {
            newVectors.addAll(fieldVectors);
            newVectors.add(vector);
        } else {
            for (int i = 0; i < fieldVectors.size(); i++) {
                if (i == index) {
                    newVectors.add(vector);
                }
                newVectors.add(fieldVectors.get(i));
            }
        }
        return newVectors;
    }

    /**
     * Returns a new List of FieldVectors created by removing the selected Vector from the list in this Table.
     *
     * @param index field index
     * @return out List of FieldVectos like the list in this table, but with the argument removed
     */
    List<FieldVector> extractVector(int index) {
        Preconditions.checkArgument(index >= 0 && index < fieldVectors.size());
        List<FieldVector> newVectors = new ArrayList<>();
        for (int i = 0; i < fieldVectors.size(); i++) {
            if (i != index) {
                newVectors.add(fieldVectors.get(i));
            }
        }
        return newVectors;
    }

    /**
     * Returns the number of vectors (columns) in this table
     */
    public int getVectorCount() {
        return fieldVectors.size();
    }

    void clear() {
        close();
        rowCount = 0;
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
     * Returns a new VectorSchemaRoot with the data and schema from this table. Data is transferred to the new
     * VectorSchemaRoot, so this table is cleared and the rowCount is set to 0;
     *
     * @return a new VectorSchemaRoot
     */
    public VectorSchemaRoot toVectorSchemaRoot() {
        VectorSchemaRoot vsr = new VectorSchemaRoot(
                fieldVectors.stream().map(v -> {
                    TransferPair transferPair = v.getTransferPair(v.getAllocator());
                    transferPair.transfer();
                    return (FieldVector) transferPair.getTo();
                }).collect(Collectors.toList())
        );
        clear();
        return vsr;
    }

    /**
     * Returns the vector with the given name, or {@code null} if the name is not found. Names are case-sensitive.
     *
     * @param columnName   The name of the vector
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

    public Cursor immutableCursor() {
        return new Cursor(this);
    }

    public Cursor immutableCursor(Charset defaultCharset) {
        return new Cursor(this, defaultCharset);
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
     * Slice this table from desired index. Memory is NOT transferred from the vectors in this table to new vectors in
     * the target table. This table is unchanged.
     *
     * @param index start position of the slice
     * @return the sliced table
     */
    public abstract BaseTable slice(int index);

    /**
     * Slice this table at desired index and length. Memory is NOT transferred from the vectors in this table to new
     * vectors in the target table. This table is unchanged.
     *
     * @param index start position of the slice
     * @param length length of the slice
     * @return the sliced table
     */
    public abstract BaseTable slice(int index, int length);

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
