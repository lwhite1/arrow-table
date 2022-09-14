package org.apache.arrow.table;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.apache.arrow.table.TestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

class MutableCursorTest {

    private BufferAllocator allocator;

    @BeforeEach
    public void init() {
        allocator = new RootAllocator(Long.MAX_VALUE);
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
            c.setPosition(1);
            assertEquals(c.getRowNumber(), 1);
            assertEquals(2, c.getInt(0));
        }
    }

    @Test
    void setNullByColumnIndex() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (MutableTable t = new MutableTable(vectorList)) {
            MutableCursor c = t.mutableCursor();
            c.setPosition(1);
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
            c.setPosition(1);
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
            c.setPosition(1);
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
            c.setPosition(1);
            assertNotEquals(132, c.getInt(INT_VECTOR_NAME_1));
            c.setInt(INT_VECTOR_NAME_1, 132);
            assertEquals(132, c.getInt(INT_VECTOR_NAME_1));
        }
    }

    @Test
    void setUInt4ByColumnName() {
        List<FieldVector> vectorList = numericVectors(allocator, 2);
        try (MutableTable t = new MutableTable(vectorList)) {
            MutableCursor c = t.mutableCursor();
            c.setPosition(1);
            c.setUInt4("uInt4_vector", 132);
            int result = c.getUInt4("uInt4_vector");
            assertEquals(132, result);
        }
    }

    @Test
    void setVarCharByColumnIndex() {
        List<FieldVector> vectorList = intPlusVarcharColumns(allocator);
        try (MutableTable t = new MutableTable(vectorList)) {
            assertEquals(2, t.rowCount);
            MutableCursor c = t.mutableCursor();
            c.setPosition(1);
            assertEquals(2, c.getInt(0));
            assertEquals("two", c.getVarChar(1));
            c.setVarChar(1, "2");
            c.setPosition(1);
            assertTrue(c.isRowDeleted());
            c.setPosition(2);
            assertEquals("2", c.getVarChar(1));
        }
    }

    @Test
    void setVarCharByColumnName() {
        List<FieldVector> vectorList = intPlusVarcharColumns(allocator);
        try (MutableTable t = new MutableTable(vectorList)) {
            assertEquals(2, t.rowCount);
            MutableCursor c = t.mutableCursor();
            c.setPosition(1);
            assertEquals(2, c.getInt(0));
            assertEquals("two", c.getVarChar(VARCHAR_VECTOR_NAME_1));
            c.setVarChar(VARCHAR_VECTOR_NAME_1, "2");
            c.setPosition(1);
            assertTrue(c.isRowDeleted());
            c.setPosition(2);
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
    void setVarCharByColumnNameUsingDictionary() {
        List<FieldVector> vectorList = intPlusVarcharColumns(allocator);
        VarCharVector v1 = (VarCharVector) vectorList.get(1);
        Dictionary numbersDictionary = new Dictionary(v1,
                new DictionaryEncoding(1L, false, new ArrowType.Int(8, true)));

        try (MutableTable t = new MutableTable(vectorList)) {
            assertEquals(2, t.rowCount);
            MutableCursor c = t.mutableCursor();
            c.setPosition(1);
            assertEquals(2, c.getInt(0));
            assertEquals("two", c.getVarChar(VARCHAR_VECTOR_NAME_1));
            c.setVarChar(VARCHAR_VECTOR_NAME_1, "2");
            c.setPosition(1);
            assertTrue(c.isRowDeleted());
            c.setPosition(2);
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
            c.setPosition(0);
            assertFalse(c.isRowDeleted());
            c.deleteCurrentRow();
            assertTrue(c.isRowDeleted());
            assertTrue(t.isRowDeleted(0));
        }
    }

    @Test
    void compact() {

    }
}