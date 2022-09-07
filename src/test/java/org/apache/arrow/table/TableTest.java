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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

import static org.apache.arrow.table.TestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

class TableTest {

    private final ArrowType intArrowType = new ArrowType.Int(32, true);
    private final FieldType intFieldType = new FieldType(true, intArrowType, null);

    private BufferAllocator allocator;

    @BeforeEach
    public void init() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Test
    void of() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = Table.of(vectorList.toArray(new FieldVector[2]))) {
            Cursor c = t.immutableCursor();
            assertEquals(2, t.getRowCount());
            assertEquals(2, t.getVectorCount());
            IntVector intVector1 = (IntVector) vectorList.get(0);
            assertEquals(INT_VECTOR_NAME_1, intVector1.getName());
            c.at(0);

            // Now test changes to the first vector
            // first Table value is 1
            assertEquals(1, c.getInt(INT_VECTOR_NAME_1));

            // original vector is updated to set first value to 44
            intVector1.setSafe(0, 44);
            assertEquals(44, intVector1.get(0));

            // first Table value is still 1 for the zeroth vector
            assertEquals(1, c.getInt(0));
        }
    }

    @Test
    void constructor() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList, 2)) {
            assertEquals(2, t.getRowCount());
            assertEquals(2, t.getVectorCount());
            Cursor c = t.immutableCursor();
            IntVector intVector1 = (IntVector) vectorList.get(0);
            c.at(0);

            // Now test changes to the first vector
            // first Table value is 1
            assertEquals(1, c.getInt(INT_VECTOR_NAME_1));

            // original vector is updated to set first value to 44
            intVector1.setSafe(0, 44);
            assertEquals(44, intVector1.get(0));
            assertEquals(44, ((IntVector) vectorList.get(0)).get(0));

            // first Table value is still 1 for the zeroth vector
            assertEquals(1, c.getInt(INT_VECTOR_NAME_1));
        }
    }

    @Test
    void addVector() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            IntVector v3 = new IntVector("3", intFieldType, allocator);
            Table t2 = t.addVector(2, v3);
            assertEquals(3, t2.fieldVectors.size());
            assertTrue(t2.getVector("3").isNull(0));
            assertTrue(t2.getVector("3").isNull(1));
            t2.close();
        }
    }

    @Test
    void removeVector() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        IntVector v2 = (IntVector) vectorList.get(1);
        int val1 = v2.get(0);
        int val2 = v2.get(1);
        try (Table t = new Table(vectorList)) {

            Table t2 = t.removeVector(0);
            assertEquals(1, t2.fieldVectors.size());
            assertEquals(val1, ((IntVector) t2.getVector(0)).get(0));
            assertEquals(val2, ((IntVector) t2.getVector(0)).get(1));
        }
    }

    /**
     * Tests table iterator in enhanced for loop
     */
    @Test
    void iterator1() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            Iterator<Cursor> iterator = t.iterator();
            assertNotNull(iterator);
            assertTrue(iterator.hasNext());
            int sum = 0;
            for (Cursor row: t) {
                sum += row.getInt(0);
            }
            assertEquals(3, sum);
        }
    }

    /**
     * Tests explicit iterator
     */
    @SuppressWarnings("WhileLoopReplaceableByForEach")
    @Test
    void iterator2() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            Iterator<Cursor> iterator = t.iterator();
            assertNotNull(iterator);
            assertTrue(iterator.hasNext());
            int sum = 0;
            Iterator<Cursor> it = t.iterator();
            while (it.hasNext()) {
                Cursor row = it.next();
                sum += row.getInt(0);
            }
            assertEquals(3, sum);
        }
    }

    @Test
    void toImmutableTable() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            assertEquals(t, t.toImmutableTable());
        }
    }

    @Test
    void toMutableTable() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            assertNotNull(t.getVector(INT_VECTOR_NAME_1));
            assertNotNull(t.getVector(INT_VECTOR_NAME_2));
            MutableTable mutableTable = t.toMutableTable();
            assertNotNull(mutableTable.getVector(INT_VECTOR_NAME_1));
            assertNotNull(mutableTable.getVector(INT_VECTOR_NAME_2));
            assertEquals(t.getSchema().findField(INT_VECTOR_NAME_1), mutableTable.getSchema().findField(INT_VECTOR_NAME_1));
        }
    }

    /**
     * Tests a slice operation where no length is provided, so the range extends to the end of the table
     */
    @Test
    void sliceToEnd() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            Table slice = t.slice(1);
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
        try (Table t = new Table(vectorList)) {
            Table slice = t.slice(1, 1);
            assertEquals(1, slice.rowCount);
            assertEquals(2, t.rowCount); // memory is copied for slice, not transferred
            slice.close();
        }
    }

    /**
     * Tests creation of a table from a vectorSchemaRoot
     *
     * Also tests that updates to the source Vectors do not impact the values in the Table
     */
    @Test
    void constructFromVsr() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (VectorSchemaRoot vsr = new VectorSchemaRoot(vectorList)) {
            Table t = new Table(vsr);
            Cursor c = t.immutableCursor();
            assertEquals(2, t.rowCount);
            assertEquals(0, vsr.getRowCount()); // memory is copied for slice, not transferred
            IntVector intVector1 = (IntVector) vectorList.get(0);
            c.at(0);

            // Now test changes to the first vector
            // first Table value is 1
            assertEquals(1, c.getInt(INT_VECTOR_NAME_1));

            // original vector is updated to set first value to 44
            intVector1.setSafe(0, 44);
            assertEquals(44, intVector1.get(0));
            assertEquals(44, ((IntVector) vsr.getVector(0)).get(0));

            // first Table value is still 1 for the zeroth vector
            assertEquals(1, c.getInt(INT_VECTOR_NAME_1));

            // TEST FIELDS //
            Schema schema = t.schema;
            Field f1 = t.getField(INT_VECTOR_NAME_1);
            FieldVector fv1 = vectorList.get(0);
            assertEquals(f1, fv1.getField());
            assertEquals(f1, schema.findField(INT_VECTOR_NAME_1));
            t.close();
        }
    }
}