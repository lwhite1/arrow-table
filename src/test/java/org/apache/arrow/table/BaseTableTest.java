package org.apache.arrow.table;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
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
        fieldList.add(new Field("intCol", intFieldType, null));
        fieldList.add(new Field("varCharColNoDictionary", intFieldType, null));

        schema1 = new Schema(fieldList);
    }

    @Test
    void allocateNew() {
    }

    @Test
    void getReader() {
    }

    @Test
    void testGetReader() {
    }

    @Test
    void testGetReader1() {
    }

    @Test
    void getSchema() {
        MutableTable t = MutableTable.create(schema1, allocator);
        assertEquals(schema1, t.getSchema());
    }

    @Test
    void insertVector() {
    }

    @Test
    void extractVector() {
    }

    @Test
    void close() {
    }

    @Test
    void getRowCount() {
    }

    @Test
    void toVectorSchemaRoot() {
    }

    @Test
    void getVector() {
    }

    @Test
    void testGetVector() {
    }

    @Test
    void immutableCursor() {
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
            v.setValueCount(3);  // This doesn't work
            //t.setRowCount(3);
            List<Integer> values = new ArrayList<>();
            for (MutableCursor r : t) {
                values.add(r.getInt("count"));
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