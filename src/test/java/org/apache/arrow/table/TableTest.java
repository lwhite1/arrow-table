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
    void addVector() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            IntVector v3 = new IntVector("3", intFieldType, allocator);
            Table t2 = t.addVector(2, v3);
            System.out.println(t2.getSchema().getFields());
            assertEquals(3, t2.fieldVectors.size());
            assertEquals(v3, t2.getVector(2));
        }
    }

    @Test
    void removeVector() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            IntVector v1 = (IntVector) t.getVector(0);
            v1.set(0, 1);
            v1.set(1, 2);
            IntVector v2 = (IntVector) t.getVector(1);
            v2.set(0, 3);
            v2.set(1, 4);
            Table t2 = t.removeVector(0);
            assertEquals(1, t2.fieldVectors.size());
            assertEquals(v2, t2.getVector(0));
        }
    }

    /**
     * Tests iterator construction. Cursor tests cover iterator functions
     */
    @Test
    void iterator() {
        try (MutableTable t = MutableTable.create(schema1, allocator)) {
            t.allocateNew();
            IntVector v1 = (IntVector) t.getVector(0);
            v1.set(0, 1);
            v1.set(1, 2);
            Iterator<MutableCursor> iterator = t.iterator();
            assertNotNull(iterator);
        }
    }
}