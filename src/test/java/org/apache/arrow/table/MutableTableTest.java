package org.apache.arrow.table;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.arrow.table.TestUtils.twoIntColumns;
import static org.junit.jupiter.api.Assertions.*;

class MutableTableTest {

    private static final String INT_VECTOR_NAME = "intCol";

    private final ArrowType intArrowType = new ArrowType.Int(32, true);
    private final FieldType intFieldType = new FieldType(true, intArrowType, null);

    private BufferAllocator allocator;
    private Schema schema1;

    @BeforeEach
    public void init() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        List<Field> fieldList = new ArrayList<>();
        fieldList.add(new Field(INT_VECTOR_NAME, intFieldType, null));
        schema1 = new Schema(fieldList);
    }

    @AfterEach
    public void terminate() {
        allocator.close();
    }

    @Test
    void create() {
        MutableTable t = MutableTable.create(schema1, allocator);
        assertNotNull(t);
        assertEquals(0, t.getRowCount());
    }

    @Test
    void of() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (MutableTable t = MutableTable.of(vectorList.toArray(new FieldVector[2]))) {
            assertEquals(2, t.getRowCount());
            assertEquals(2, t.getVectorCount());
        }
    }

    @Test
    void constructor() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        List<Field> fieldList = new ArrayList<>();
        for (FieldVector v : vectorList) {
            fieldList.add(v.getField());
        }
        try (MutableTable t = new MutableTable(fieldList, vectorList, 2)) {
            assertEquals(2, t.getRowCount());
            assertEquals(2, t.getVectorCount());
        }
    }

    @Test
    void allocateNew() {
    }

    @Test
    void clear() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (MutableTable t = MutableTable.of(vectorList.toArray(new FieldVector[2]))) {
            assertEquals(2, t.getRowCount());
            assertEquals(2, t.getVectorCount());
            t.clear();
            assertEquals(0, t.getRowCount());
            assertEquals(0, t.getVector(0).getValueCount());
        }
    }

    @Test
    void compact() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (MutableTable t = new MutableTable(vectorList)) {
            MutableCursor c = t.mutableCursor();
            c.at(0);
            assertFalse(c.isRowDeleted());
            c.deleteCurrentRow();
            assertTrue(c.isRowDeleted());
            assertTrue(t.isRowDeleted(0));
            t.compact();
            assertEquals(0, t.deletedRowCount());
            assertEquals(1, t.rowCount);
        }
    }

    @Test
    void compactTableWithNoDeletedRows() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (MutableTable t = MutableTable.of(vectorList.toArray(new FieldVector[2]))) {
            int initialRowCount = t.rowCount;
            int initialDeletedRowCount = t.deletedRowCount();
            t.mutableCursor().compact();
            assertEquals(initialRowCount, t.rowCount);
            assertEquals(initialDeletedRowCount, t.deletedRowCount());
        }
    }

    @Test
    void setGetRowCount() {
    }

    @Test
    void markRowDeleted() {
        try (MutableTable t = MutableTable.create(schema1, allocator)) {
            t.allocateNew();
            IntVector v = (IntVector) t.getVector(0);
            v.set(0, 1);
            v.set(1, 2);
            v.set(2, 3);
            t.setRowCount(3);
            MutableCursor c = t.mutableCursor();
            c.at(1).deleteCurrentRow();
            assertTrue(t.isRowDeleted(1));

            c.at(-1);
            List<Integer> values = new ArrayList<>();
            while(c.hasNext()) {
                c.next();
                values.add(c.getInt(0));
            }
            assertEquals(2, values.size());
            assertTrue(values.contains(1));
            assertTrue(values.contains(3));
        }
    }

    @Test
    void getVectorByName() {
        MutableTable t = MutableTable.create(schema1, allocator);
        FieldVector v = t.getVector(INT_VECTOR_NAME);
        assertEquals(INT_VECTOR_NAME, v.getName());
    }

    @Test
    void getVectorByPosition() {
        MutableTable t = MutableTable.create(schema1, allocator);
        FieldVector v = t.getVector(0);
        assertEquals(INT_VECTOR_NAME, v.getName());
    }

    @Test
    void addVector() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (MutableTable t = new MutableTable(vectorList)) {
            IntVector v3 = new IntVector("3", intFieldType, allocator);
            MutableTable t2 = t.addVector(2, v3);
            assertEquals(3, t2.fieldVectors.size());
            assertEquals(v3, t2.getVector(2));
            t2.close();
        }
    }

    @Test
    void removeVector() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (MutableTable t = new MutableTable(vectorList)) {
            IntVector v2 = (IntVector) t.getVector(1);
            MutableTable t2 = t.removeVector(0);
            assertEquals(1, t2.fieldVectors.size());
            assertEquals(v2, t2.getVector(0));
        }
    }

    @Test
    void toMutableTable() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (MutableTable t = new MutableTable(vectorList)) {
            assertEquals(t, t.toMutableTable());
        }
    }

    /**
     * Tests a slice operation where no length is provided, so the range extends to the end of the table
     */
    @Test
    void sliceToEnd() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (MutableTable t = new MutableTable(vectorList)) {
            MutableTable slice = t.slice(1);
            assertEquals(1, slice.rowCount);
            assertEquals(2, t.rowCount); // memory is copied for slice, not transferred
            slice.close();
        }
    }

    /**
     * Tests a slice operation with a given length parameter
     */
    @Test
    void sliceRange() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (MutableTable t = new MutableTable(vectorList)) {
            MutableTable slice = t.slice(1, 1);
            assertEquals(1, slice.rowCount);
            assertEquals(2, t.rowCount); // memory is copied for slice, not transferred
            slice.close();
        }
    }

    /**
     * Tests creation of a table from a vectorSchemaRoot
     */
    @Test
    void constructFromVsr() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (VectorSchemaRoot vsr = new VectorSchemaRoot(vectorList)) {
            MutableTable table = MutableTable.from(vsr);
            assertEquals(2, table.rowCount);
            assertEquals(0, vsr.getRowCount()); // memory is copied for slice, not transferred
            table.close();
        }
    }

    /**
     * Tests creation of a table from multiple vectorSchemaRoots
     */
    @Test
    void concatenateVsr() {
        List<FieldVector> vectorList1 = twoIntColumns(allocator);
        List<FieldVector> vectorList2 = twoIntColumns(allocator);
        try (
                VectorSchemaRoot vsr = new VectorSchemaRoot(vectorList1);
                VectorSchemaRoot vsr2 = new VectorSchemaRoot(vectorList2);
        ) {
            List<VectorSchemaRoot> roots = new ArrayList<>();
            roots.add(vsr);
            roots.add(vsr2);
            MutableTable table = MutableTable.concatenate(allocator, roots);
            assertEquals(4, table.rowCount);
            table.close();
        }
    }

    @Test
    void iterator() {
        try (MutableTable t = MutableTable.create(schema1, allocator)) {
            t.allocateNew();
            IntVector v = (IntVector) t.getVector(0);
            v.set(0, 1);
            v.set(1, 2);
            v.set(2, 3);
            t.setRowCount(3);
            List<Integer> values = new ArrayList<>();
            for (MutableCursor r : t) {
                values.add(r.getInt(INT_VECTOR_NAME));
            }
            assertEquals(3, values.size());
            assertTrue(values.containsAll(List.of(1, 2, 3)));
        }
    }
}