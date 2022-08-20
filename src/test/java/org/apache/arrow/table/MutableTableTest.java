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

import static org.junit.jupiter.api.Assertions.*;

class MutableTableTest {

    private BufferAllocator allocator;
    private Schema schema1;

    @BeforeEach
    public void init() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        List<Field> fieldList = new ArrayList<>();
        ArrowType arrowType =  new ArrowType.Int(32,true);
        FieldType intFieldType = new FieldType(true, arrowType, null);
        fieldList.add(new Field("count", intFieldType, null));
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
    }

    @Test
    void allocateNew() {
    }

    @Test
    void clear() {

    }

    @Test
    void getSchema() {
        MutableTable t = MutableTable.create(schema1, allocator);
        assertEquals(schema1, t.getSchema());
    }

    @Test
    void testCreate() {
    }

    @Test
    void testOf() {
    }

    @Test
    void testAllocateNew() {
    }

    @Test
    void setAndGetName() {
        MutableTable t = MutableTable.create(schema1, allocator);
        t.setName("test1");
        assertEquals("test1", t.getName());
    }

    @Test
    void compact() {
    }

    @Test
    void getReader() {
    }

    @Test
    void getReader1() {
    }

    @Test
    void addVector() {
    }

    @Test
    void removeVector() {
    }

    @Test
    void close() {
    }

    @Test
    void setGetRowCount() {
    }

    @Test
    void markRowDeleted() {
    }

    @Test
    void getVectorByName() {
        MutableTable t = MutableTable.create(schema1, allocator);
        FieldVector v = t.getVector("count");
        assertEquals("count", v.getName());
    }

    @Test
    void getVectorByPosition() {
        MutableTable t = MutableTable.create(schema1, allocator);
        FieldVector v = t.getVector(0);
        assertEquals("count", v.getName());
    }

    @Test
    void iterator() {
        try (MutableTable t = MutableTable.create(schema1, allocator)) {
            t.allocateNew();
            IntVector v = (IntVector) t.getVector(0);
            v.set(0, 1);
            v.set(1, 2);
            v.set(2, 3);
            v.setValueCount(3);  // This doesn't work
            t.setRowCount(3);
            List<Integer> values = new ArrayList<>();
            for (MutableCursor r : t) {
                values.add(r.getInt("count"));
            }
            assertEquals(3, values.size());
            assertTrue(values.containsAll(List.of(1, 2, 3)));
            System.out.println("pass");
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
            v.setValueCount(3);  // This doesn't work
            t.setRowCount(3);
            List<Integer> values = new ArrayList<>();
            for (MutableCursor r : t) {
                values.add(r.getInt("count"));
            }
            assertEquals(3, values.size());
            assertTrue(values.containsAll(List.of(1, 2, 3)));
            System.out.println(t.contentToTSVString());
        }
    }
}