package org.apache.arrow.table;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.apache.arrow.table.TestUtils.*;
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
    void constructor() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            Cursor c = t.immutableCursor(StandardCharsets.US_ASCII);
            assertEquals(StandardCharsets.US_ASCII, c.getDefaultCharacterSet());
        }
    }

    @Test
    void at() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            Cursor c = t.immutableCursor();
            assertEquals(c.getRowNumber(), -1);
            c.at(1);
            assertEquals(c.getRowNumber(), 1);
        }
    }

    @Test
    void getIntByVectorIndex() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            Cursor c = t.immutableCursor();
            c.at(1);
            assertEquals(2, c.getInt(0));
        }
    }

    @Test
    void getIntByVectorName() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            Cursor c = t.immutableCursor();
            c.at(1);
            assertEquals(2, c.getInt(INT_VECTOR_NAME_1));
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
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            Cursor c = t.immutableCursor();
            c.at(0);
            c.next();
            assertEquals(1, c.getRowNumber());
        }
    }

    @Test
    void isNull() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            Cursor c = t.immutableCursor();
            c.at(1);
            assertFalse(c.isNull(0));
        }
    }

    @Test
    void isNullByFieldName() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList)) {
            Cursor c = t.immutableCursor();
            c.at(1);
            assertFalse(c.isNull(INT_VECTOR_NAME_1));
        }
    }

    @Test
    void fixedWidthVectorTest() {
        List<FieldVector> vectorList = fixedWidthVectors(allocator, 2);
        try (Table t = new Table(vectorList)) {
            Cursor c = t.immutableCursor();
            c.at(1);
            assertFalse(c.isNull("bigInt_vector"));
            assertEquals(c.getInt("int_vector"), c.getInt(0));
            assertEquals(c.getBigInt("bigInt_vector"), c.getBigInt(1));
            assertEquals(c.getSmallInt("smallInt_vector"), c.getSmallInt(2));
            assertEquals(c.getTinyInt("tinyInt_vector"), c.getTinyInt(3));

            // TODO: Uncomment these when GenerateSampleData supports UInts
            //assertEquals(c.getUInt1("uInt1_vector"), c.getUInt1(0));
            //assertEquals(c.getUInt2("uInt2_vector"), c.getUInt2(1));
            //assertEquals(c.getUInt4("uInt4_vector"), c.getUInt4(2));
            //assertEquals(c.getUInt8("uInt8_vector"), c.getUInt8(3));

            assertEquals(c.getFloat4("float4_vector"), c.getFloat4(8));
            assertEquals(c.getFloat8("float8_vector"), c.getFloat8(9));

            assertEquals(c.getTimeSec("timeSec_vector"), c.getTimeSec(10));
            assertEquals(c.getTimeMilli("timeMilli_vector"), c.getTimeMilli(11));
            assertEquals(c.getTimeMicro("timeMicro_vector"), c.getTimeMicro(12));
            assertEquals(c.getTimeNano("timeNano_vector"), c.getTimeNano(13));

            assertEquals(c.getTimeStampSec("timeStampSec_vector"), c.getTimeStampSec(14));
            assertEquals(c.getTimeStampMilli("timeStampMilli_vector"), c.getTimeStampMilli(15));
            assertEquals(c.getTimeStampMicro("timeStampMicro_vector"), c.getTimeStampMicro(16));
            assertEquals(c.getTimeStampNano("timeStampNano_vector"), c.getTimeStampNano(17));
        }
    }
}