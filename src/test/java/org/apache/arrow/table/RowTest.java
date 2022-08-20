package org.apache.arrow.table;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
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

class RowTest {

    private BufferAllocator allocator;
    private Schema schema1;

    @BeforeEach
    public void init() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        List<Field> fieldList = new ArrayList<>();
        ArrowType arrowType =  new ArrowType.Int(32,true);
        FieldType fieldType = new FieldType(true, arrowType, null);
        fieldList.add(new Field("count", fieldType, null));
        schema1 = new Schema(fieldList);
    }

    @AfterEach
    public void terminate() {
        allocator.close();
    }

    @Test
    void at() {
    }

    @Test
    void isMissing() {
    }

    @Test
    void getInt() {
    }

    @Test
    void testGetInt() {
    }

    @Test
    void setInt() {
    }

    @Test
    void testSetInt() {
    }

    @Test
    void hasNext() {
    }

    @Test
    void next() {
    }
}