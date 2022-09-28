# Table

**NOTE**: The API is experimental and subject to change.

*Table* is a new immutable tabular data structure based on FieldVectors. A mutable version (*MutableTable*) is expected in a subsequent release. This document describes the Table API and identifies limitations in the current release. 

---

Like VectorSchemaRoot, *Table* and *MutableTable* are columnar data structures backed by Arrow arrays, or more specifically, by FieldVector objects. They differ from VectorSchemaRoot mainly in that they lack its support for batch operations. Anyone processing batches of tabular data in a pipeline should continue to use VectorSchemaRoot.

## Mutation in Table and VectorSchemaRoot

VectorSchemaRoot provides a thin wrapper on the FieldVectors that hold its data. Individual FieldVectors can be retrieved from the VectorSchemaRoot. These FieldVectors have *setters* for modifying their elements, so VectorSchemaRoot is immutable only by convention. The protocol for mutating a vector is documented in the ValueVector class: 

- values need to be written in order (e.g. index 0, 1, 2, 5)
- null vectors start with all values as null before writing anything
- for variable width types, the offset vector should be all zeros before writing
- you must call setValueCount before a vector can be read
- you should never write to a vector once it has been read.

The rules aren't enforced by the API so the programmer is responsible for ensuring they're followed. Failure to do so could lead to runtime exceptions. 

_Table_, on the other hand, is actually immutable. The underlying vectors are not exposed. When a Table is created from existing vectors, their memory is transferred to new vectors, so subsequent changes to the original vectors can't impact the new table's values.

## What's in a Table?

Like VectorSchemaRoot, Table consists of a `Schema` and an ordered collection of `FieldVector` objects, but it is designed to be accessed via a row-oriented interface.

## Table API: Creating Tables

### Creating a Table from a VectorSchemaRoot

Tables are created from a VectorSchemaRoot as shown below. The memory buffers holding the data are transferred from the VectorSchemaRoot to new vectors in the new Table, clearing the original VectorSchemaRoot in the process. This ensures that the data in your new Table is never changed. Since the buffers are transferred rather than copied, this is a very low overhead operation. 

```java
VectorSchemaRoot vsr = getMyVsr(); 
Table t = new Table(vsr);
```

If you now update the FieldVectors used to create the VectorSchemaRoot (using some variation of  `ValueVector#setSafe()`), the VectorSchemaRoot *vsr* would reflect those changes, but the values in Table *t* are unchanged. 

***Current Limitation:*** Due to an unresolved limitation in `CDataReferenceManager`, you cannot currently create a Table from a VectorSchemRoot that was created in native code and transferred to Java via the C-Data Interfacee.  

### Creating a Table from ValueVectors

Tables can be created from ValueVectors as shown below.

```java
IntVector myVector = createMyIntVector(); 
VectorSchemaRoot vsr1 = new VectorSchemaRoot(myVector); 
```

or 

```java
IntVector myVector = createMyIntVector(); 
List<FieldVector> fvList = List.of(myVector);
VectorSchemaRoot vsr1 = new VectorSchemaRoot(fvList); 
```

It is rarely a good idea to share vectors between multiple VectorSchemaRoots, and it would not be a good idea to share them between VectorSchemaRoots and tables. Creating a VectorSchemaRoot from a list of vectors does not cause the reference counts for the vectors to be incremented. Unless you manage the counts manually, the code shown below would lead to more references to the vectors than reference counts, and that could lead to trouble. There is an implicit assumption that the vectors were created for use by *one* VectorSchemaRoot that this code violates.  

*Don't do this:*

```Java
IntVector myVector = createMyIntVector();  // Reference count for myVector = 1
VectorSchemaRoot vsr1 = new VectorSchemaRoot(myVector); // Still one reference
VectorSchemaRoot vsr2 = new VectorSchemaRoot(myVector); 
// Ref count is still one, but there are two VSRs with a reference to myVector
vsr2.clear(); // Reference count for myVector is 0.
```

What is happening is that the reference counter works at a lower level than the VectorSchemaRoot interface. A reference counter counts references to ArrowBuf instances that control memory buffers. It doesn't count references to the ValueVectors that hold *them*. In the examaple above, each ArrowBuf is held by one ValueVector, so there is only one reference. This distinction is blurred though, when you call the VectorSchemaRoot's clear() method, which frees the memory held by each of the vectors it references even though another instance might refer to the same vectors. 

When you create Tables from vectors, it's assumed that there are no external references to those vectors. But, just to be on the safe side, the buffers underlying these vectors are transferred to new ValueVectors in the new Table, and the original vectors are cleared.  

*Don't do this either, but note the difference from above:*

```Java
IntVector myVector = createMyIntVector(); // Reference count for myVector = 1
Table t1 = new Table(myVector);  // myVector is cleared; Table t1 has a new hidden vector with
																 // the data from myVector
Table t2 = new Table(myVector);  // t2 has no rows because the myVector was just cleared
																 // t1 continues to have the data from the original vector
t2.clear();                      // no change because t2 is already empty and t1 is independent
```

With Tables, memory is explicitly transferred on instantiatlon so the buffers are held by that table are held by *only* that table. 

#### Creating Tables with dictionary-encoded vectors

***Note: this section is highly speculative***

Another point of difference is that dictionary-encoding is managed separately from VectorSchemaRoot, while Tables hold an optional DictionaryProvider instance. If any vectors in the source data are encoded, a DictionaryProvider must be set to un-encode the values.  

```java
VectorSchemaRoot vsr = myVsr(); 
DictionaryProvider provider = myProvider();
Table t = new Table(vsr, provider);
```

In the immutable Table case, dictionaries are used in a way that's similar to the approach used with ValueVectors. To decode a vector, the user provides the dictionary id and the name of the vector to decode:

```Java
Table t = new Table(vsr, provider);
ValueVector decodedName = t.decode("name", 1L);
```

To encode a vector from a table, a similar approach is used:

```Java
Table t = new Table(vsr, provider);
ValueVector encodedName = t.encode("name", 1L);
```

***Current Limitation:*** One difference is the method that produces TSV formatted output has an extra switch instructing the Table to replace the encoded output with the decoded output where possible:

```java
String output = myTable.contentToTSVString(true);
```

### Freeing memory explicitly

Tables use off-heap memory that must be freed when it is no longer needed. Table implements AutoCloseable so the best way to create one is in a try-with-resources block: 

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

## Table API: Adding and removing vectors

Both Table and MutableTable provide facilities for adding and removing FieldVectors modeled on the same functionality in VectorSchemaRoot. As with VectorSchemaRoot. These operations return new instances rather than modifiying the original instance in-place.

```java
try (Table t = new Table(vectorList)) {
  IntVector v3 = new IntVector("3", intFieldType, allocator);
  Table t2 = t.addVector(2, v3);
  Table t3 = t2.removeVector(1);
  // don't forget to close t2 and t3
}
```

## Table API: Slicing tables

Table supports *slice()* operations, where a slice of a source table is a second Table that refers to a single, contiguous range of rows in the source. 

```Java
try (Table t = new Table(vectorList)) {
  Table t2 = t.slice(100, 200); // creates a slice referencing the values in range (100, 200]
  ...
}
```

If you created a slice with *all* the values in the source table (as shown below), how would that differ from a new Table constructed with the same vectors as the source?

```Java
try (Table t = new Table(vectorList)) {
  Table t2 = t.slice(0, t.getRowCount()); // creates a slice referencing all the values in t
  // ...
}
```

The difference is that when you *construct* a new table, the buffers are transferred from the source vectors to new vectors in the destination. With a slice, both tables share the same underlying vectors. That's OK, though, since both Tables are immutable.  

Slices will not be supported in MutableTables. 

## Table API: Row operations

Row-based access is supported using a Cursor object. Cursor provides *get()* methods by both vector name and vector position, but no *set()* operations. 

**Note**: A mutable cursor implementation is expected with the release of MutableTable, which will support both mutable and immutable cursors.

### Getting a cursor

Calling `immutableCursor()` on any table instance returns a cursor supporting read operations.

```java
Cursor c = table.immutableCursor(); 
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

Table implements `Iterable<Cursor>` so you can access rows directly from Table in an enhanced *for* loop:

```java
for (Cursor row: table) {
  int age = row.getInt("age");
  boolean nameIsNull = row.isNull("name");
  ... 
}
```

Finally, while cursors are usually iterated in the order of the underlying data vectors, but they are also positionable using the `Cursor#setPosition()` method, so you can skip to a specific row. Row numbers are 0-based. 

```java
Cursor c = table.immutableCursor(); 
int age101 = c.setPosition(101); // change position directly to 101
```

Any changes to position are of course applied to all the columns in the table. 

Note that you must call `next()`, or `setPosition()` before accessing values via a cursor. Failure to do so results in a runtime exception. 

### Read operations using cursors

Methods are available for getting values by vector name and vector index, where index is the 0-based position of the vector in the table. For example, assuming 'age' is the 13th vector in 'table', the following two gets are equivalent:

```java
Cursor c = table.immutableCursor(); 
c.next(); // position the cursor at the first value
int age1 = c.get("age"); // gets the value of vector named 'age' in the table at row 0
int age2 = c.get(12);    // gets the value of the 13th vecto in the table at row 0
```

You can also get value using a NullableHolder. For example: 

```Java
NullableIntHolder holder = new NullableIntHolder(); 
int b = cursor.getInt("age", holder);
```

This can be used to retrieve values without creating a new Object for each. 

In addition to getting values, you can check if a value is null using `isNull()` and you can get the current row number: 

```java
boolean name0isNull = cursor.isNull("name");
int row = cursor.getRowNumber(); 
```

Note that while there are getters for most vector types (e.g. *getInt()* for use with IntVector) and a generic *isNull()* method, there is no *getNull()* method for use with the NullVector type or *getZero()* for use with ZeroVector (a zero-length vector of any type).

#### Reading values as Objects

For any given vector type, the basic *get()* method returns a primitive value wherever possible. For example, *getTimeStampMicro()* returns a long value that encodes the timestamp. To get the LocalDateTime object representing that timestamp in Java, another method with 'Obj' appended to the name is provided.  For example: 

```java
long ts = cursor.getTimeStampMicro();
LocalDateTime tsObject = cursor.getTimeStampMicroObj();
```

#### Reading VarChars and LargeVarChars

Strings in arrow are represented as byte arrays, encoded with a particular Charset object. There are two ways to handle Charset in the getters. One uses the default Charset; the other takes a charset as an argument to the getter:

```Java
String v1 = cursor.get("first_name");  // uses the default encoding for the table

String v2 = cursor.get("first_name", StandardCharsets.US_ASCII); // specifies the encoding
```

What the default coding *is* will depend on how the Cursor was constructed. If you use:

```Java
Cursor c = table.immutableCursor(); 
```

Then the default encoding is set as StandardCharsets.UTF_8.  However, you can also provide a default charset when you create the cursor. 

```java
Cursor c = table.immutableCursor(StandardCharsets.US_ASCII); 
// or
MutableCursor c = table.mutableCursor(StandardCharsets.US_ASCII); 
```

Now US_ASCII will be used whenever you get a String value without specifying a Charset in the getter.  

## Table API: Converting a Table to a VectorSchemaRoot

Tables can be converted to VectorSchemaRoot objects using the *toVectorSchemaRoot()* method. 

```java
VectorSchemaRoot root = myTable.toVectorSchemaRoot();
```

Buffers are transferred to the VectorSchemaRoot and the Table is cleared.

## Table API: Working with the Streaming API and the C-Data interface

The ability to work with native code is required for many Arrow features. This section describes how tables can be be exported and imported using two mechanisms.

### Using the Streaming API with Tables

***Current limitation: Streaming API is not currently supported.***

### Using the C-Data interface with Tables

***Current limitation: Data transferred from native code using the C-Data-interface cannot be used in a table, because the current implementation of CdataReferenceManager does not support the transfer operation.*** 