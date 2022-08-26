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
import org.junit.platform.commons.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

import static org.apache.arrow.table.TestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

class BaseTableTest {

    private BufferAllocator allocator;
    private Schema schema1;

    @BeforeEach
    public void init() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        List<Field> fieldList = new ArrayList<>();
        ArrowType intArrowType =  new ArrowType.Int(32,true);
        ArrowType binaryArrowType = new ArrowType.Binary();
        FieldType intFieldType = new FieldType(true, intArrowType, null);
        FieldType vcFieldType = new FieldType(true, binaryArrowType, null);
        fieldList.add(new Field(INT_VECTOR_NAME, intFieldType, null));
        fieldList.add(new Field("varCharColNoDictionary", vcFieldType, null));

        schema1 = new Schema(fieldList);
    }

    @Test
    void getReaderByName() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            assertNotNull(t.getReader(INT_VECTOR_NAME_1));
        }
    }

    @Test
    void getReaderByIndex() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            assertNotNull(t.getReader(0));
        }
    }

    @Test
    void getReaderByField() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            assertNotNull(t.getReader(t.getField(INT_VECTOR_NAME_1)));
        }
    }

    @Test
    void getSchema() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            assertNotNull(t.getSchema());
            assertEquals(2, t.getSchema().getFields().size());
        }
    }

    @Test
    void insertVector() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            ArrowType intArrowType =  new ArrowType.Int(32,true);
            FieldType intFieldType = new FieldType(true, intArrowType, null);
            IntVector v3 = new IntVector("3", intFieldType, allocator);
            List<FieldVector> revisedVectors = t.insertVector(2, v3);
            assertEquals(3, revisedVectors.size());
            assertEquals(v3, revisedVectors.get(2));
        }
    }

    @Test
    void insertVectorFirstPosition() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            ArrowType intArrowType =  new ArrowType.Int(32,true);
            FieldType intFieldType = new FieldType(true, intArrowType, null);
            IntVector v3 = new IntVector("3", intFieldType, allocator);
            List<FieldVector> revisedVectors = t.insertVector(0, v3);
            assertEquals(3, revisedVectors.size());
            assertEquals(v3, revisedVectors.get(0));
        }
    }

    @Test
    void extractVector() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            List<FieldVector> revisedVectors= t.extractVector(0);
            assertEquals(2, t.getVectorCount()); // vector not removed from table yet
            assertEquals(1, revisedVectors.size());
        }
    }

    @Test
    void close() {
        IntVector v = new IntVector(INT_VECTOR_NAME, allocator);
        v.setSafe(0, 132);
        List<FieldVector> vectors = new ArrayList<>();
        vectors.add(v);
        v.setValueCount(1);
        try (Table t = new Table(vectors)) {
            t.close();
            for (FieldVector fieldVector: t.fieldVectors) {
                assertEquals(0, fieldVector.getValueCount());
            }
        }
    }

    @Test
    void getRowCount() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            // TODO: handle setting rowcount on Table construction
            assertEquals(2, t.getRowCount());
        }
    }

    @Test
    void toVectorSchemaRoot() {
    }

    @Test
    void getVector() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            assertNotNull(t.getVector(0));
        }
    }

    @Test
    void testGetVector() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            assertNotNull(t.getVector(INT_VECTOR_NAME_1));
            assertNull(t.getVector("foobar"));
        }
    }

    @Test
    void immutableCursor() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            assertNotNull(t.immutableCursor());
        }
    }

    @Test
    void contentToTsvString() {
        try (MutableTable t = MutableTable.create(schema1, allocator)) {
            t.allocateNew();
            IntVector v = (IntVector) t.getVector(0);
            v.set(0, 1);
            v.set(1, 2);
            v.set(2, 3);
            assertEquals(2, v.get(1));
            t.setRowCount(3);
            List<Integer> values = new ArrayList<>();
            for (MutableCursor r : t) {
                values.add(r.getInt(INT_VECTOR_NAME));
            }
            assertEquals(3, values.size());
            assertTrue(values.containsAll(List.of(1, 2, 3)));
            System.out.println(t.contentToTSVString());
        }
    }


    @Test
    void slice() {
    }

    @Test
    void testSlice() {
    }

    @Test
    void isDeletedRow() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            assertFalse(t.isDeletedRow(0));
            assertFalse(t.isDeletedRow(1));
        }
    }
}