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

class CursorTest {

    public static final String INT_VECTOR_NAME = "intCol";
    private BufferAllocator allocator;
    private Schema schema1;

    @BeforeEach
    public void init() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        List<Field> fieldList = new ArrayList<>();
        ArrowType arrowType =  new ArrowType.Int(32,true);
        FieldType fieldType = new FieldType(true, arrowType, null);
        fieldList.add(new Field(INT_VECTOR_NAME, fieldType, null));
        schema1 = new Schema(fieldList);
    }

    @AfterEach
    public void terminate() {
        allocator.close();
    }

    @Test
    void at() {
        try (MutableTable t = MutableTable.create(schema1, allocator)) {
            t.allocateNew();
            IntVector v = (IntVector) t.getVector(0);
            v.set(0, 1);
            v.set(1, 2);
            v.set(2, 3);
            Cursor c = t.immutableCursor();
            assertEquals(c.getRowNumber(), -1);
            c.at(1);
            assertEquals(c.getRowNumber(), 1);
            assertEquals(2, c.getInt(0));
        }
    }

    @Test
    void getIntByVectorIndex() {
        try (MutableTable t = MutableTable.create(schema1, allocator)) {
            t.allocateNew();
            IntVector v = (IntVector) t.getVector(0);
            v.set(0, 1);
            v.set(1, 2);
            Cursor c = t.immutableCursor();
            c.at(1);
            assertEquals(2, c.getInt(0));
        }
    }

    @Test
    void getIntByVectorName() {
        try (MutableTable t = MutableTable.create(schema1, allocator)) {
            t.allocateNew();
            IntVector v = (IntVector) t.getVector(0);
            v.set(0, 1);
            v.set(1, 2);
            Cursor c = t.immutableCursor();
            c.at(1);
            assertEquals(2, c.getInt(INT_VECTOR_NAME));
        }
    }

    @Test
    void hasNext() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            Cursor c = t.immutableCursor();
            assertTrue(c.hasNext());
            c.at(1);
            assertFalse(c.hasNext());
        }
    }

    @Test
    void next() {
    }
}