

# Working with Tables

**NOTE**: The API described in this document is experimental and subject to change.

Table and MutableTable are tabular data structures backed by Arrow arrays. They are similar to VectorSchemaRoot, 
but lack its support for batch operations. Anyone managing batches of tabular data (in a pipeline, for example) would likely be better off continuing to use a VectorSchemaRoot. Table and MutableTable also differ from VectorSchemaRoot in their mutation semantics.

## Mutation semantics

VectorSchemaRoot provides a thin wrapper on the FieldVectors has hold its data. While these FieldVectors have setters, they must be used in a particular way, as described in ValueVector JavaDoc: 

- values need to be written in order (e.g. index 0, 1, 2, 5)
- null vectors start with all values as null before writing anything
- for variable width types, the offset vector should be all zeros before writing
- you must call setValueCount before a vector can be read
- you should never write to a vector once it has been read.

As these rules are not enforced by the API, it's up to the programmer to ensure they are not violated. Failure to do so could lead to runtime exceptions. *Table* and *MutableTable* take a different approach:

_Table_ is (almost) entirely immutable. The underlying vectors are not exposed. They can only be modified by keeping a reference to the arrays that are passed to the Table constructor, and updating those outside of the Table class, which should be avoided. Whenever possible, Tables should be created using a constructor that transfers the memory to the table.

_MutableTable_, on the other hand, has more general mutation support than VectorSchemaRoot. Values of any ArrowType can be modified at any time and in any order. However, because it is ultimately backed by Arrow arrays, the mutation process may be complex and less efficient than might be desired. Mutation is described in more detail below in the _Write Operations_ section.

## What's in a Table?
Both Table and MutableTable consist largely of a Schema and a collection of FieldVector objects. Both Table and MutableTable are designed to be accessed via positionable Cursor objects that provide a row-oriented interface.

## Creating a Table

The preferred way to create a Table is from a VectorSchemaRoot using the static method *from()*. This method transfers the data from the VectorSchemaRoot to the new Table, clearing the VectorSchemaRoot in the process. The benefit of this approach is that you can trust that the data in your new Table is never changed. An example is provided below:

```java
VectorSchemaRoot vsr = myMethodForGettingVsrs(); 
Table t = Table.from(vsr);
```

To create a MutableTable, a similar method is provided:

```java
VectorSchemaRoot vsr = myMethodForGettingVsrs(); 
MutableTable t = MutableTable.from(vsr);
```

MutableTables don't provide the benefits of immutability of course, but they have a more general and safer approach to mutability than VectorSchemaRoot, so they may be preferable for some applications. 

### Creating a Table with dictionary encoded vectors

Another point of difference from VectorSchemaRoot is that Tables hold an optional DictionaryProvider instance. If any vectors in the source data are dictionary encoded, a DictionaryProvider that can be used to un-encode the values must be provided. 

```java
VectorSchemaRoot vsr = myVsr(); 
DictionaryProvider provider = myProvider();
Table t = Table.from(vsr, provider);
```

## Managing Table memory

Remember that Tables use off-heap memory that must be explicitly freed when it is no longer needed. Table implements AutoCloseable so the best way to create one is in a try-with-resources block: 

```java
try (VectorSchemaRoot vsr = myMethodForGettingVsrs(); 
		 MutableTable t = MutableTable.from(vsr)) {
		 // do useful things.
}
```

If you don't use a try-with-resources block, you must close the Table manually

````java
try {
  VectorSchemaRoot vsr = myMethodForGettingVsrs(); 
  MutableTable t = MutableTable.from(vsr);
  // do useful things.
} finally {
  vsr.close();
  t.close();
}
````

Manually closing should be performed in a finally block.

## Adding and removing Vectors

Both Table and MutableTable provide facilities for adding and removing FieldVectors that are modeled on the same functionality in VectorSchemaRoot. As with VectorSchemaRoot. These operations return new instances of the structure rather than modifiying them in-place.

```java
try (Table t = new Table(vectorList)) {
  IntVector v3 = new IntVector("3", intFieldType, allocator);
  Table t2 = t.addVector(2, v3);
  Table t3 = t2.removeVector(1);
}
```

## Row operations

Row-based access is supported using a Cursor object. Cursor provides *get()* operations by both vector name and vector position, but no *set()* operations. A MutableCursor provides both *set()* and *get()* operations. If you are working with a MutableTable, you can use either a MutableCursor or an immutable Cursor to access the data.

### Getting a cursor

Call `Table#immutableCursor` to get a cursor supporting only read operations.

```java
Cursor c = table.immutableCursor(); 
```

This works for either mutable or immutable tables. If your table is a MutableTable, you can get a cursor that can be used to write to the table using the `mutableCursor()` method:

```java
MutableCursor mc = mutableTable.mutableCursor(); 
```

### Getting around

Cursors are usually iterated in the order of the underlying data vectors, but they are also positionable, so you can skip to a specific row. Row numbers are 0-based. 

```java
Cursor c = table.immutableCursor(); 
int age101 = c.at(101) // change position directly to 101
```

Since cursors are itearble, you can traverse a table using a standard while loop:

```java
Cursor c = table.immutableCursor(); 
while (c.hasNext()) {
  c.next();
  // do something useful here
}
```

Table implements `Iterable<Cursor>` so you can access rows in an enhanced *for* loop:

```java
for (Cursor row: table) {
  int age = row.getInt("age");
  boolean nameIsNull = row.isNull("name");
  ... 
}
```

MutableTable implements `Iterable<MutableCursor>` rather than `Iterable<Cursor>`,  which provides write access:

```java
for (MutableCursor row: mutableable) {
  int age = row.getInt("age");
  row.setNull("name");
  ... 
}
```

If you need an ImmutableCursor for your MutableTable, you can get one as described above.

### Read operations

```java
Cursor c = table.immutableCursor(); 
int age101 = c.get("age");
boolean name101isNull = c.isNull("name");
int row = c.getRowNumber(); // 101
```

### Write operations

A MutableTables can be modified through its MutableCursor. You can:

- append records
- update values in any row
- delete any row

These operations can be performed at any time and in any order. 

Because MutableCursor also implements the public API for Cursor, the *get()* methods are available, and the read and write operations can be interleaved arbitrarily. For example:

```java
MutableCursor c = mutableTable.mutableCursor();
while (c.hasNext()) {
  c.next();
  if (!c.isNull()) {             // a read operation
    if (c.getInt("age") >= 18) { // another read operation
        c.setNull("chaparone")   // some write operations
            .setVarChar("status", "adult");
    }
  }
}
```

As you can see in the above example:

- You can check for null values using *isNull()*
- You can set a value to null using *setNull()*
- You can get a value using *get...()*
- You can set a value using *set...()*

You must specifiy which column to operate on. You can specifiy a column using its position in the table or its name. When you get or set any non-null value, you must also specify the type of vector you're working with as part of the method name, for example `getVarChar()`. The type portion of the name will match the vector's type, so that you would use *getInt()* to get a value from an IntVector, *getUInt8()* to get a long value from a UInt8Vector (an unsigned 8 byte integer vector), and *getVarChar()* to get a String value from a VarCharVector. 

All accessor methods return the cursor itself so they can be chained together. 

#### How mutation works

This section describes the mutation API and implementation. It's important to have some understanding of the mutation mechanism so that you understand the performance, safety, and reliability implications of changing Arrow data values. 

##### Deletions

To delete a row, set the cursor to the row that you want deleted and call `deleteCurrentRow()`. To delete all the even-numbered rows in a MutableTable, for example, you could do this: 

```java
for (MutableCursor row: mutableTable) {
  if (row.getRowNumber() % 2 == 0) {
    row.deleteCurrentRow(); 
  }
}
```

Deletions are performed virtually. The row to be deleted is marked as such, but remains in the table until compaction occurs. The number of deleted records is available using `Table#deletedRowCount()`. This may be useful in deciding when to compact. Once a row is marked as deleted, it is skipped on subsequent iterations, but it can be accessed using `MutableTable#at(rowIndex)`. The following code checks whether a row is deleted, and deletes it if it isn't: 

```java
MutableCursor mc = mutableTable.mutableCursor();
mc.at(140); // positions the cursor at row 140
if (!mc.isDeletedRow()) {
  mc.deleteCurrentRow();
}
```

Note that extreme care must be taken when undo-ing deletes when updates are involved. See the section on updates for more information. 

###### Compaction

The compaction process removes any rows marked for deletion. To perform compaction, simply call the compact() method:

```java
int deleted = oldTable.deletedRowCount(); 
oldTable.compact();
int newDeleted = oldTable.deletedRowCount(); // the deleted row count is now 0
```

**NOTE**: Compaction may be an expensive process as it involves moving the values in every vector: When compact() is called, the values in each vector are scanned, and once the first deleted row is found, each subsequent value must be moved "up" in the table to fill-in the space previously alloted to the deleted row value(s). 

##### Updates

Updates are performed using *set()* methods.

There are two ways that updates are handled. They are done in place whenever possible. For example, updates to any fixed-width vector are always performed in-place. Otherwise, they are performed by deleting the original row, and appending a new copy of that row with the value changed.

###### Updating dictionary-encoded fields

Updates to dictionary-encoded fields may be more efficient....

***TODO: write this section when code is available***

###### Performing multiple updates in one pass

Please note that setting two variable-width vectors in a single row (see below) may result in multiple deletions and insertions.

```java
mutableCursor.setVarChar("first_name", "John").setVarChar("last_name", "Doe");
```

If neither of the vectors use dictionary encoding, updating both in the same row would involve two operations. 

To avoid this problem, the method *setAll()* is provided. *setAll()* takes a map of vector names to ValueHolders to perform multiple updates in the same row with a single delete/append row occuring behind the scenes. For example: 

```java
Map<String, ValueHolder> valueMap = new HashMap();
map.put("first_name", fNameHolder);
map.put("last_name", lNameHolder)l
mutableCurser.setAll(valueMap);
```

Note that the createion of ValueHolders for complex types is not entirely trivial, but only updates that lead to a delete-and-append operation benefit from this approach. Updates performed in-place can be handled using the individual  setters (e.g. *setInt(102);*), and the two approaches may be combined to modify any row. 

##### Appending new rows

***TODO: this section is highly speculative. Add example code when available***

The *appendRow()* operation does two things:

1. Ensures that there is space in each vector to write a new row.
2. Positions the row index at the new row 

After calling appendRow(), updates can be perfomed as usual.  See the section on pefroming multiple operations on a single row for performance considerations.

##### Limitations and unsupported operations

Table does not currently support insertAt(index, value) operations, nor does it guarantee that row order remains consistent after update operations.  Both of these operations would be useful for anyone building an Arrow-native dataframe. 

## Converting to a VectorSchemaRoot

Tables can be converted to VectorSchemaRoot objects using the *toVectorSchemaRoot()* method. 

```java
VectorSchemaRoot root = myTable.toVectorSchemaRoot();
```

## Working with the Streaming API and the C-Data interface

The ability to work with native code is required for many Arrow features. This section describes how tables can be be exported and imported using two mechanisms.

### Using the Streaming API with Tables

***TODO: write this section when code is available***

### Using the C-Data interface with Tables

Currently, Table usage of the C-Data interface is mediated by VectorSchemaRoot. The following example shows a Table being exported and re-imported using the C-Data interface API for VectorSchemaRoot. 

***TODO: Consider (future?) direct C-Data support***

One concern with this approach is that using VectorSchemaRoot as an itermediary means that the static method `Table.from(VectorSchemaRoot)` cannot be used to transfer the memory from the VSR, so that it would be possible to directly access the vectors inside the 'immutable' table. (This is in-part because the implementation of CdataReferenceManager does not support the transfer operation.) Furthermore, the Table constructor new Table(VectorSchemaRoot), which creates a table that shares memory with the given VSR cannot be removed, meaning that it may be used for other use-cases where a truly immutable table is desirable. Finally, the need to go through a VSR makes it less obvious how the import/export process should be performed and makes the API a bit more complex.

```java
VectorSchemaRoot importedRoot;
Table importedTable;
try (Table t = new Table(vectorList);
     VectorSchemaRoot root = t.toVectorSchemaRoot()) {
           
  // Consumer allocates empty structures
  try (ArrowSchema consumerSchema = ArrowSchema.allocateNew(allocator);
       ArrowArray consumerArray = ArrowArray.allocateNew(allocator)) {
    // Producer creates structures from existing memory pointers
    try (ArrowSchema arrowSchema = ArrowSchema.wrap(consumerSchema.memoryAddress());
         ArrowArray arrowArray = ArrowArray.wrap(consumerArray.memoryAddress())) {
      // Producer exports vector into the C Data Interface structures
      Data.exportVectorSchemaRoot(allocator, root, null, arrowArray, arrowSchema);
    }
    // Consumer imports vector
    importedRoot = Data.importVectorSchemaRoot(allocator, consumerArray, consumerSchema, null);
    importedTable = new Table(importedRoot);
  }
}
```

