

# Working with Tables

**NOTE**: This API is experimental and subject to change.

Like VectorSchemaRoot, *Table* and *MutableTable* are tabular data structures backed by Arrow arrays. They differ from VectorSchemaRoot mainly in that they lack its support for batch operations. Anyone processing batches of tabular data in a pipeline should continue to use VectorSchemaRoot. Table and MutableTable also differ from VectorSchemaRoot in their mutation semantics.

## Mutation semantics

VectorSchemaRoot provides a thin wrapper on the FieldVectors has hold its data. These vectors have "set" methods, and they can be accessed through the table, but their use is subject to rules documented in the ValueVector class: 

- values need to be written in order (e.g. index 0, 1, 2, 5)
- null vectors start with all values as null before writing anything
- for variable width types, the offset vector should be all zeros before writing
- you must call setValueCount before a vector can be read
- you should never write to a vector once it has been read.

The rules aren't enforced by the API so it's up to the programmer to ensure they're followed. Failure to do so could lead to runtime exceptions. 

_Table_ is immutable. The underlying vectors are not exposed. When a Table is created from existing vectors, memory is transferred so subsequent changes to the vectors don't impact the table's values.

_MutableTable_ has more general mutation support than VectorSchemaRoot. Values of any ArrowType can be modified at any time in any order. The process, however, may be too slow for some applications. Mutation is described in more detail below in the _Write Operations_ section.

## What's in a Table?
Like VectorSchemaRoot, both Table and MutableTable consist of a Schema and a collection of FieldVector objects, and both are designed to be accessed via a row-oriented interface.

## Creating Tables

### Creating a Table from a VectorSchemaRoot

Tables are created from a VectorSchemaRoot as shown below. The data is transferred from the VectorSchemaRoot to the new Table, clearing the VectorSchemaRoot in the process. This ensures that the data in your new Table is never changed.

```java
VectorSchemaRoot vsr = getMyVsr(); 
Table t = new Table(vsr);
```

If you now update the FieldVectors used to create the VectorSchemaRoot (using some variation of  `ValueVector#setSafe()`), the VectorSchemaRoot would reflect those changes, but the values in Table *t* are unchanged. 

To create a MutableTable, a similar method is provided:

```java
VectorSchemaRoot vsr = getMyVsr(); 
MutableTable t = new MutableTable(vsr);
```

Again the memory is transferred to the table. MutableTables don't provide the benefits of immutability of course, but they *are* protected from external changes.

#### Creating Tables with dictionary-encoded vectors

***TODO: this section is highly speculative. Add example code when available***

Another point of difference is that dictionary-encoding is managed separately from VectorSchemaRoot, while Tables hold an optional DictionaryProvider instance. If any vectors in the source data are encoded, a DictionaryProvider must be set to un-encode the values.  

```java
VectorSchemaRoot vsr = myVsr(); 
DictionaryProvider provider = myProvider();
Table t = new Table(vsr, provider);
```

How dictionaries are used is described below.

### Creating a Table from ValueVectors

It is rarely a good idea to share vectors between multiple VectorSchemaRoots, and it would not be a good idea to share them between VectorSchemaRoots and tables. Creating a VectorSchemaRoot from a list of vectors does not cause the reference counts for the vectors to be incremented. Unless you manage it manually, the code shown below would lead to more references than reference counts, which could lead to trouble. There is an implicit assumption that the vectors were created for use by *one* VectorSchemaRoot. 

*Don't do this:*

```Java
IntVector myVector = createMyIntVector();  // Reference count for myVector = 1
VectorSchemaRoot vsr1 = new VectorSchemaRoot(myVector); // Still one reference
VectorSchemaRoot vsr2 = new VectorSchemaRoot(myVector); 
// Ref count is still one, but there are two VSRs with a reference to myVector
vsr2.clear(); // Reference count for myVector is 0.
```

What is happening is that the reference counter works at a lower level than the VectorSchemaRoot interface. A reference counter counts references to ArrowBuf instances that control memory buffers. It doesn't count references to the ValueVectors that hold *them*. In the examaple above, each ArrowBuf is held by one ValueVector, so there is only one reference. This gets a little blurry, though, when you call the VectorSchemaRoot's clear() method, which frees the memory held by each of the vectors it references, even though another instance might refer to the same vectors. 

When you create Tables from vectors, it's assumed that there are no external references to those vectors. But, just to be on the safe side, the buffers underlying these vectors are transferred to new ValueVectors in the new Table, and the original vectors are cleared.  

*Don't do this either, but understand the difference from above:*

```Java
IntVector myVector = createMyIntVector();  // Reference count for myVector = 1
Table vsr1 = new Table(myVector);          // Still one reference
Table vsr2 = new Table(myVector); 
// Still 1. The vector held by vsr1 was cleared and its buffers transferred to a new vector.
vsr2.clear(); // Reference count for myVector is 0.
```

With Tables, memory is explicitly transferred on instantiatlon so the buffers are held by that table are held by *only* that table. 

## Freeing memory explicitly

Tables use off-heap memory that must be explicitly freed when it is no longer needed. Table implements AutoCloseable so the best way to create one is in a try-with-resources block: 

```java
try (VectorSchemaRoot vsr = myMethodForGettingVsrs(); 
		 MutableTable t = new MutableTable(vsr)) {
		 // do useful things.
}
```

If you don't use a try-with-resources block, you must close the Table manually:

````java
try {
  VectorSchemaRoot vsr = myMethodForGettingVsrs(); 
  MutableTable t = new MutableTable(vsr);
  // do useful things.
} finally {
  vsr.close();
  t.close();
}
````

Manually closing should be performed in a finally block.

## Adding and removing vectors from a table

Both Table and MutableTable provide facilities for adding and removing FieldVectors modeled on the same functionality in VectorSchemaRoot. As with VectorSchemaRoot. These operations return new instances rather than modifiying the original instance in-place.

```java
try (Table t = new Table(vectorList)) {
  IntVector v3 = new IntVector("3", intFieldType, allocator);
  Table t2 = t.addVector(2, v3);
  Table t3 = t2.removeVector(1);
  // don't forget to close t2 and t3
}
```

## Slicing tables

Both Table and MutableTable support *slice()* operations. A slice of a Table is another Table, and a slice of a MutableTable is a MutableTable. 

```Java
try (Table t = new Table(vectorList)) {
  Table t2 = t.slice(100, 200); // creates a slice referencing the values in range (100, 200]
  // do something useful with t2, and don't forget to close it explicitly
}
```

What if you created a slice of *all* the values in the original table?

```Java
try (Table t = new Table(vectorList)) {
  Table t2 = t.slice(0, t.getRowCount()); // creates a slice referencing all the values in t
  // ...
}
```

The difference between creating a slice with all the data in the source Table, and constructing a new Table with the same vectors as the source Table, is that when you *construct* a new table, the buffers are transferred from the source to the destination. With a slice, both tables share the same underlying vectors. That's OK, though, since both Tables are immutable.  

### Slicing a MutableTable

***TODO: this section is highly speculative. Add example code when available***

## Row operations

Row-based access is supported using a Cursor object. Cursor provides *get()* methods by both vector name and vector position, but no *set()* operations. A MutableCursor provides both *set()* and *get()* operations. MutableCursors are only available for MutableTables. If you are working with a MutableTable, however, you can use either a MutableCursor or an immutable Cursor to access the data.

### Getting a cursor

Call `Table#immutableCursor` to get a cursor supporting only read operations.

```java
Cursor c = table.immutableCursor(); 
```

As mentioned, this works for either mutable or immutable tables. If you have a MutableTable, you can get a cursor to write to the table using the `mutableCursor()` method:

```java
MutableCursor mc = mutableTable.mutableCursor(); 
```

### Getting around

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

Finally, while cursors are usually iterated in the order of the underlying data vectors, but they are also positionable using the `Cursor#at()` method, so you can skip to a specific row. Row numbers are 0-based. 

```java
Cursor c = table.immutableCursor(); 
int age101 = c.at(101) // change position directly to 101
```

### Read operations using cursors

In addition to getting values, you can check if a value is null using `isNull()`. 

```java
Cursor c = table.immutableCursor(); 
int age101 = c.get("age");
boolean name101isNull = c.isNull("name");
int row = c.getRowNumber(); // 101
```

### Write operations using cursors

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

## Converting a Table to a VectorSchemaRoot

Tables can be converted to VectorSchemaRoot objects using the *toVectorSchemaRoot()* method. 

```java
VectorSchemaRoot root = myTable.toVectorSchemaRoot();
```

Buffers are transferred to the VectorSchemaRoot and the Table is cleared.

## Working with the Streaming API and the C-Data interface

The ability to work with native code is required for many Arrow features. This section describes how tables can be be exported and imported using two mechanisms.

### Using the Streaming API with Tables

***TODO: write this section when code is available***

### Using the C-Data interface with Tables

Currently, Table usage of the C-Data interface is mediated by VectorSchemaRoot. The following example shows a Table being exported and re-imported using the C-Data interface API for VectorSchemaRoot. 

***TODO: Consider (future?) direct C-Data support***

One concern with this approach is that using VectorSchemaRoot as an itermediary means that the constructor: `new Table(VectorSchemaRoot)` cannot be used to transfer the memory from the VSR, so that it would be possible to directly access the vectors inside the 'immutable' table. (This is because the current implementation of CdataReferenceManager does not support the transfer operation.) Furthermore, the Table constructor new Table(VectorSchemaRoot), which creates a table that shares memory with the given VSR cannot be removed, meaning that it may be used for other use-cases where a truly immutable table is desirable. Finally, the need to go through a VSR makes it less obvious how the import/export process should be performed and makes the API a bit more complex.

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

