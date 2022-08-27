package org.apache.arrow.table;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.arrow.table.TestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

class TableTest {

    private final ArrowType intArrowType = new ArrowType.Int(32, true);
    private final FieldType intFieldType = new FieldType(true, intArrowType, null);

    private BufferAllocator allocator;
    private Schema schema1;

    @BeforeEach
    public void init() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        List<Field> fieldList = new ArrayList<>();
        fieldList.add(new Field(INT_VECTOR_NAME_1, intFieldType, null));
        fieldList.add(new Field(INT_VECTOR_NAME_2, intFieldType, null));
        schema1 = new Schema(fieldList);
    }

    @Test
    void of() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = Table.of(vectorList.toArray(new FieldVector[2]))) {
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
        try (Table t = new Table(fieldList, vectorList, 2)) {
            assertEquals(2, t.getRowCount());
            assertEquals(2, t.getVectorCount());
        }
    }

    @Test
    void addVector() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            IntVector v3 = new IntVector("3", intFieldType, allocator);
            Table t2 = t.addVector(2, v3);
            assertEquals(3, t2.fieldVectors.size());
            assertEquals(v3, t2.getVector(2));
            t2.close();
        }
    }

    @Test
    void removeVector() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            IntVector v2 = (IntVector) t.getVector(1);
            Table t2 = t.removeVector(0);
            assertEquals(1, t2.fieldVectors.size());
            assertEquals(v2, t2.getVector(0));
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

    }
}