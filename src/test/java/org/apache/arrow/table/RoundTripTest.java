package org.apache.arrow.table;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.arrow.table.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RoundTripTest {

    private BufferAllocator allocator;

    @BeforeEach
    public void init() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Test
    void roundTrip1() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        try (Table t = new Table(vectorList);
             VectorSchemaRoot root = t.toVectorSchemaRoot()) {
            VectorSchemaRoot importedRoot = vectorSchemaRootRoundtrip(root);

            // Verify correctness
            assertEquals(root.getSchema(), importedRoot.getSchema());
            Table importedTable = new Table(importedRoot);
            assertEquals(t.getSchema(), importedTable.getSchema());
            assertEquals(2, importedTable.getRowCount());
        }
    }

    @Test
    void roundTrip2() {
        List<FieldVector> vectorList = twoIntColumns(allocator);
        VectorSchemaRoot importedRoot;
        Table importedTable;

        try (Table t = new Table(vectorList);
             VectorSchemaRoot root = t.toVectorSchemaRoot()) {

            // Consumer allocates empty structures
            try (ArrowSchema consumerArrowSchema = ArrowSchema.allocateNew(allocator);
                 ArrowArray consumerArrowArray = ArrowArray.allocateNew(allocator)) {

                // Producer creates structures from existing memory pointers
                try (ArrowSchema arrowSchema = ArrowSchema.wrap(consumerArrowSchema.memoryAddress());
                     ArrowArray arrowArray = ArrowArray.wrap(consumerArrowArray.memoryAddress())) {
                    // Producer exports vector into the C Data Interface structures
                    Data.exportVectorSchemaRoot(allocator, root, null, arrowArray, arrowSchema);
                }

                // Consumer imports vector
                importedRoot = Data.importVectorSchemaRoot(allocator, consumerArrowArray, consumerArrowSchema, null);
                importedTable = new Table(importedRoot);
            }
            importedRoot.close();
            importedTable.close();
        }
    }

    /**
     * Exports and imports the same VSR
     * @param root  The VSR to export
     * @return      The re-imported VSR
     */
    private VectorSchemaRoot vectorSchemaRootRoundtrip(VectorSchemaRoot root) {
        // Consumer allocates empty structures
        try (ArrowSchema consumerArrowSchema = ArrowSchema.allocateNew(allocator);
             ArrowArray consumerArrowArray = ArrowArray.allocateNew(allocator)) {

            // Producer creates structures from existing memory pointers
            try (ArrowSchema arrowSchema = ArrowSchema.wrap(consumerArrowSchema.memoryAddress());
                 ArrowArray arrowArray = ArrowArray.wrap(consumerArrowArray.memoryAddress())) {
                // Producer exports vector into the C Data Interface structures
                Data.exportVectorSchemaRoot(allocator, root, null, arrowArray, arrowSchema);
            }

            // Consumer imports vector
            return Data.importVectorSchemaRoot(allocator, consumerArrowArray, consumerArrowSchema, null);
        }
    }

}
