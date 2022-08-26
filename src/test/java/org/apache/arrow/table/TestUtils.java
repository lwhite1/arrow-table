package org.apache.arrow.table;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.junit.platform.commons.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

public class TestUtils {

    public static final String INT_VECTOR_NAME = "intCol";
    public static final String INT_VECTOR_NAME_1 = "intCol1";
    public static final String INT_VECTOR_NAME_2 = "intCol2";

    /**
     * Returns a list of two IntVectors to be used to instantiate Tables for testing
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

}
