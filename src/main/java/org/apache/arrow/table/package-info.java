package org.apache.arrow.table;

/*
 * Table provides interactive, row-based access to a tabular dataset.
 *
 * Instances of the Table class are immutable.
 * Instances of the MutableTable class are mutable, with simpler and more general semantics than {@link org.apache.arrow.vector.VectorSchemaRoot},
 * which is a tabular data structure optimized for batch processing.
 *
 * The main type in this package is Table.
 *
 * Row-based access to Table data is provided by the Cursor type.
 * Mutable row-based access is provided by the MutableCursor type. Table provides access
 * to the immutable Cursor, via the immutableCursor() method. MutableTable also provides
 * immutable (read) access via the immutableCursor() method, and mutable (write) access
 * via the mutableCursor() method. A MutableCursor contains both read and write (get and set)
 * methods, so they can be combined.
 *
 * To iterate a Table directly, you can do so @code{ for (Cursor row : table)}.
 *
 * When iterating a MutableTable, MutableCursor is the iterator: @code{ for MutableCursor row : table)}.
 *
 */