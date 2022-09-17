package org.apache.arrow.table;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.writer.Float8Writer;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.platform.commons.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

import static org.apache.arrow.vector.complex.BaseRepeatedValueVector.OFFSET_WIDTH;

public class TestUtils {

    public static final String INT_VECTOR_NAME = "intCol";
    public static final String VARCHAR_VECTOR_NAME = "varcharCol";
    public static final String INT_VECTOR_NAME_1 = "intCol1";
    public static final String VARCHAR_VECTOR_NAME_1 = "varcharCol1";
    public static final String INT_VECTOR_NAME_2 = "intCol2";
    public static final String INT_LIST_VECTOR_NAME = "int list vector";
    public static final String INT_DOUBLE_MAP_VECTOR_NAME = "int-double map vector";
    public static final String STRUCT_VECTOR_NAME = "struct_vector";

    /**
     * Returns a list of two IntVectors to be used to instantiate Tables for testing.
     * Each IntVector has two values set
     */
    static List<FieldVector> twoIntColumns(BufferAllocator allocator) {
        List<FieldVector> vectorList = new ArrayList<>();
        Preconditions.condition(allocator != null, "allocator is null");
        IntVector v1 = new IntVector(INT_VECTOR_NAME_1, allocator);
        v1.allocateNew(2);
        v1.set(0, 1);
        v1.set(1, 2);
        v1.setValueCount(2);
        IntVector v2 = new IntVector(INT_VECTOR_NAME_2, allocator);
        v2.allocateNew(2);
        v2.set(0, 3);
        v2.set(1, 4);
        v2.setValueCount(2);
        vectorList.add(v1);
        vectorList.add(v2);
        return vectorList;
    }

    /**
     * Returns a list of two FieldVectors to be used to instantiate Tables for testing.
     * The first vector is an IntVector and the second is a VarCharVector.
     * Each vector has two values set.
     */
    static List<FieldVector> intPlusVarcharColumns(BufferAllocator allocator) {
        List<FieldVector> vectorList = new ArrayList<>();
        Preconditions.condition(allocator != null, "allocator is null");
        IntVector v1 = new IntVector(INT_VECTOR_NAME_1, allocator);
        v1.allocateNew(2);
        v1.set(0, 1);
        v1.set(1, 2);
        v1.setValueCount(2);
        VarCharVector v2 = new VarCharVector(VARCHAR_VECTOR_NAME_1, allocator);
        v2.allocateNew(2);
        v2.set(0, "one".getBytes());
        v2.set(1, "two".getBytes());
        v2.setValueCount(2);
        vectorList.add(v1);
        vectorList.add(v2);
        return vectorList;
    }

    static List<FieldVector> fixedWidthVectors(BufferAllocator allocator, int rowCount) {
        List<FieldVector> vectors = new ArrayList<>();
        numericVectors(vectors, allocator, rowCount);
        return simpleTemporalVectors(vectors, allocator, rowCount);
    }

    static List<FieldVector> numericVectors(List<FieldVector> vectors, BufferAllocator allocator, int rowCount) {
        vectors.add(new IntVector("int_vector", allocator));
        vectors.add(new BigIntVector("bigInt_vector", allocator));
        vectors.add(new SmallIntVector("smallInt_vector", allocator));
        vectors.add(new TinyIntVector("tinyInt_vector", allocator));
        vectors.add(new UInt1Vector("uInt1_vector", allocator));
        vectors.add(new UInt2Vector("uInt2_vector", allocator));
        vectors.add(new UInt4Vector("uInt4_vector", allocator));
        vectors.add(new UInt8Vector("uInt8_vector", allocator));
        vectors.add(new Float4Vector("float4_vector", allocator));
        vectors.add(new Float8Vector("float8_vector", allocator));
        vectors.forEach(vec -> GenerateSampleData.generateTestData(vec, rowCount));
        return vectors;
    }

    static List<FieldVector> numericVectors(BufferAllocator allocator, int rowCount) {
        List<FieldVector> vectors = new ArrayList<>();
        return numericVectors(vectors, allocator, rowCount);
    }

    static List<FieldVector> simpleTemporalVectors(List<FieldVector> vectors, BufferAllocator allocator, int rowCount) {
        vectors.add(new TimeSecVector("timeSec_vector", allocator));
        vectors.add(new TimeMilliVector("timeMilli_vector", allocator));
        vectors.add(new TimeMicroVector("timeMicro_vector", allocator));
        vectors.add(new TimeNanoVector("timeNano_vector", allocator));

        vectors.add(new TimeStampSecVector("timeStampSec_vector", allocator));
        vectors.add(new TimeStampMilliVector("timeStampMilli_vector", allocator));
        vectors.add(new TimeStampMicroVector("timeStampMicro_vector", allocator));
        vectors.add(new TimeStampNanoVector("timeStampNano_vector", allocator));

        vectors.forEach(vec -> GenerateSampleData.generateTestData(vec, rowCount));
        return vectors;
    }

    static List<FieldVector> simpleTemporalVectors(BufferAllocator allocator, int rowCount) {
        List<FieldVector> vectors = new ArrayList<>();
        return simpleTemporalVectors(vectors, allocator, rowCount);
    }

    /**
     * Returns a list vector of ints
     */
    static ListVector simpleListVector(BufferAllocator allocator) {
        ListVector listVector = ListVector.empty(INT_LIST_VECTOR_NAME, allocator);
        final int innerCount = 80; // total number of values
        final int outerCount = 8;  // total number of values in the list vector itself
        final int listLength = innerCount / outerCount; // length of an individual list

        Types.MinorType type = Types.MinorType.INT;
        listVector.addOrGetVector(FieldType.nullable(type.getType()));

        listVector.allocateNew();
        IntVector dataVector = (IntVector) listVector.getDataVector();

        for (int i = 0; i < innerCount; i++) {
            dataVector.set(i, i);
        }
        dataVector.setValueCount(innerCount);

        for (int i = 0; i < outerCount; i++) {
            BitVectorHelper.setBit(listVector.getValidityBuffer(), i);
            listVector.getOffsetBuffer().setInt(i * OFFSET_WIDTH, i * listLength);
            listVector.getOffsetBuffer().setInt((i + 1) * OFFSET_WIDTH, (i + 1) * listLength);
        }
        listVector.setLastSet(outerCount - 1);
        listVector.setValueCount(outerCount);

        return listVector;
    }


    static StructVector simpleStructVector(BufferAllocator allocator) {
        final String INT_COL = "struct_int_child";
        final String FLT_COL = "struct_flt_child";
        StructVector structVector = StructVector.empty(STRUCT_VECTOR_NAME, allocator);
        final int size = 6; // number of structs

        NullableStructWriter structWriter = structVector.getWriter();
        structVector.addOrGet(INT_COL, FieldType.nullable(Types.MinorType.INT.getType()), IntVector.class);
        structVector.addOrGet(FLT_COL, FieldType.nullable(Types.MinorType.INT.getType()), IntVector.class);
        structVector.allocateNew();
        IntWriter intWriter = structWriter.integer(INT_COL);
        Float8Writer float8Writer = structWriter.float8(FLT_COL);

        for (int i = 0; i < size; i++) {
            structWriter.setPosition(i);
            structWriter.start();
            intWriter.writeInt(i);
            float8Writer.writeFloat8(i * .1);
            structWriter.end();
        }

        structWriter.setValueCount(size);

        return structVector;
    }

    /**
     * Returns a MapVector of ints to doubles
     */
    static MapVector simpleMapVector(BufferAllocator allocator) {
        MapVector mapVector = MapVector.empty("map", allocator, false);
        mapVector.allocateNew();
        int count = 5;
        UnionMapWriter mapWriter = mapVector.getWriter();
        for (int i = 0; i < count; i++) {
            mapWriter.startMap();
            for (int j = 0; j < i + 1; j++) {
                mapWriter.startEntry();
                mapWriter.key().bigInt().writeBigInt(j);
                mapWriter.value().integer().writeInt(j);
                mapWriter.endEntry();
            }
            mapWriter.endMap();
        }
        mapWriter.setValueCount(count);
        return mapVector;
    }
}
