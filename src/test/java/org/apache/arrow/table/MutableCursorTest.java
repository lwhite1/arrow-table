package org.apache.arrow.table;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.arrow.table.TestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

class MutableCursorTest {

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
    void at() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (MutableTable t = new MutableTable(vectorList)) {
            Cursor c = t.immutableCursor();
            assertEquals(c.getRowNumber(), -1);
            c.at(1);
            assertEquals(c.getRowNumber(), 1);
            assertEquals(2, c.getInt(0));
        }
    }

    @Test
    void setNullByColumnIndex() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (MutableTable t = new MutableTable(vectorList)) {
            MutableCursor c = t.mutableCursor();
            c.at(1);
            assertFalse(c.isNull(0));
            c.setNull(0);
            assertTrue(c.isNull(0));
        }
    }

    @Test
    void setIntlByColumnIndex() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (MutableTable t = new MutableTable(vectorList)) {
            MutableCursor c = t.mutableCursor();
            c.at(1);
            assertNotEquals(132, c.getInt(0));
            c.setInt(0, 132).setInt(1, 146);
            assertEquals(132, c.getInt(0));
            assertEquals(146, c.getInt(1));
        }
    }

    @Test
    void setIntlByColumnName() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (MutableTable t = new MutableTable(vectorList)) {
            MutableCursor c = t.mutableCursor();
            c.at(1);
            assertNotEquals(132, c.getInt(INT_VECTOR_NAME_1));
            c.setInt(INT_VECTOR_NAME_1, 132);
            assertEquals(132, c.getInt(INT_VECTOR_NAME_1));
        }
    }

    @Test
    void setNullByColumnName() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (MutableTable t = new MutableTable(vectorList)) {
            MutableCursor c = t.mutableCursor();
            c.at(1);
            assertFalse(c.isNull(INT_VECTOR_NAME_1));
            c.setNull(INT_VECTOR_NAME_1);
            assertTrue(c.isNull(INT_VECTOR_NAME_1));
        }
    }

    @Test
    void delete() {
    }

    @Test
    void setInt() {
    }

    @Test
    void testSetInt() {
    }

    @Test
    void setVarChar() {
    }

    @Test
    void testSetVarChar() {
    }
}