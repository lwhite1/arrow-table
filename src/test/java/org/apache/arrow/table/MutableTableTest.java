package org.apache.arrow.table;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
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

    private BufferAllocator allocator;
    private Schema schema1;

    @BeforeEach
    public void init() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        List<Field> fieldList = new ArrayList<>();
        ArrowType arrowType =  new ArrowType.Int(32,true);
        FieldType intFieldType = new FieldType(true, arrowType, null);
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
    }

    @Test
    void compactTableWithNoDeletedRows() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (MutableTable t = MutableTable.of(vectorList.toArray(new FieldVector[2]))) {
            assertEquals(t, t.compact());
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
            assertTrue(t.isDeletedRow(1));

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