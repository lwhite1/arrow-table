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

import java.nio.charset.StandardCharsets;
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
    void constructor() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (MutableTable t = new MutableTable(vectorList)) {
            Cursor c = t.mutableCursor(StandardCharsets.US_ASCII);
            assertEquals(StandardCharsets.US_ASCII, c.getDefaultCharacterSet());
        }
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
    void setIntByColumnIndex() {
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
    void setIntByColumnName() {
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
    void setVarCharByColumnIndex() {
        List<FieldVector> vectorList = intPlusVarcharColumns(allocator);
        try (MutableTable t = new MutableTable(vectorList)) {
            assertEquals(2, t.rowCount);
            MutableCursor c = t.mutableCursor();
            c.at(1);
            assertEquals(2, c.getInt(0));
            assertEquals("two", c.getVarChar(1));
            c.setVarChar(1, "2");
            c.at(1);
            assertTrue(c.isDeletedRow());
            c.at(2);
            assertEquals("2", c.getVarChar(1));
        }
    }

    @Test
    void setVarCharByColumnName() {
        List<FieldVector> vectorList = intPlusVarcharColumns(allocator);
        try (MutableTable t = new MutableTable(vectorList)) {
            assertEquals(2, t.rowCount);
            MutableCursor c = t.mutableCursor();
            c.at(1);
            assertEquals(2, c.getInt(0));
            assertEquals("two", c.getVarChar(VARCHAR_VECTOR_NAME_1));
            c.setVarChar(VARCHAR_VECTOR_NAME_1, "2");
            c.at(1);
            assertTrue(c.isDeletedRow());
            c.at(2);
            assertEquals("2", c.getVarChar(VARCHAR_VECTOR_NAME_1));

            // ensure iteration works correctly
            List<String> values = new ArrayList<>();
            c.resetPosition();
            while (c.hasNext()) {
                c.next();
                values.add(c.getVarChar(VARCHAR_VECTOR_NAME_1));
            }
            assertTrue(values.contains("one"));
            assertTrue(values.contains("2"));
            assertEquals(2, values.size());
        }
    }

    @Test
    void delete() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (MutableTable t = new MutableTable(vectorList)) {
            MutableCursor c = t.mutableCursor();
            c.at(0);
            assertFalse(c.isDeletedRow());
            c.deleteCurrentRow();
            assertTrue(c.isDeletedRow());
            assertTrue(t.isDeletedRow(0));
        }
    }

    @Test
    void compact() {

    }
}