package org.apache.arrow.table;

/*
 * The MutableTable module provides interactive, columnar, and row-based access to a tabular dataset.
 * Tables are mutable, with simpler and more general semantics than {@link org.apache.arrow.vector.VectorSchemaRoot},
 * which is a tabular data structure optimized for batch processing.
 *
 * The main type in this package is MutableTable. MutableCursor-based access is provided by the MutableCursor type.
 *
 */