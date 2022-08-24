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
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BaseTableTest {

    public static final String INT_VECTOR_NAME = "intCol";
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
        fieldList.add(new Field("varCharColNoDictionary", intFieldType, null));

        schema1 = new Schema(fieldList);
    }

    @Test
    void allocateNew() {
    }

    @Test
    void getReaderByName() {
        try(MutableTable t = MutableTable.create(schema1, allocator)) {
            assertNotNull(t.getReader(INT_VECTOR_NAME));
        }
    }

    @Test
    void getReaderByIndex() {
        try(MutableTable t = MutableTable.create(schema1, allocator)) {
            assertNotNull(t.getReader(0));
        }
    }

    @Test
    void getReaderByField() {
        try(MutableTable t = MutableTable.create(schema1, allocator)) {
            assertNotNull(t.getReader(t.getField(INT_VECTOR_NAME)));
        }
    }

    @Test
    void getSchema() {
        try(MutableTable t = MutableTable.create(schema1, allocator)) {
            assertEquals(schema1, t.getSchema());
        }
    }

    @Test
    void insertVector() {
    }

    @Test
    void extractVector() {
    }

    @Test
    void close() {
        IntVector v = new IntVector(INT_VECTOR_NAME, allocator);
        v.setSafe(0, 132);
        List<FieldVector> vectors = new ArrayList<>();
        vectors.add(v);
        v.setValueCount(1);
        try (ImmutableTable t = new ImmutableTable(vectors)) {
            t.close();
            for (FieldVector fieldVector: t.fieldVectors) {
                assertEquals(0, fieldVector.getValueCount());
            }
        }
    }

    @Test
    void getRowCount() {
        IntVector v = new IntVector(INT_VECTOR_NAME, allocator);
        v.setSafe(0, 132);

        try (ImmutableTable t = new ImmutableTable(List.of(v))) {
            // TODO: handle setting rowcount on ImmutableTable construction
            assertEquals(1, t.getRowCount());
        }
    }

    @Test
    void toVectorSchemaRoot() {
    }

    @Test
    void getVector() {
        try (ImmutableTable t = ImmutableTable.create(schema1, allocator)) {
            assertNotNull(t.getVector(0));
        }
    }

    @Test
    void testGetVector() {
        try (ImmutableTable t = ImmutableTable.create(schema1, allocator)) {
            assertNotNull(t.getVector(INT_VECTOR_NAME));
        }
    }

    @Test
    void immutableCursor() {
        try (ImmutableTable t = ImmutableTable.create(schema1, allocator)) {
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
    }
}