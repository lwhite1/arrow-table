# Table
 
NOTE: This module is experimental and subject to change.

Table and MutableTable are tabular data structures backed by Arrow arrays. They are similar to VectorSchemaRoot, 
but lack its support for batch operations. They also differ considerably in how array values are set.

## Mutation semantics

_Table_ is (almost) entirely immutable. The underlying vectors are not exposed. The only way they could be modified is 
by keeping a reference to the arrays and updating those outside of the Table class, which is unsafe and should be avoided. 

_MutableTable_, on the other hand, has more general mutation support than VectorSchemaRoot. Values of any ArrowType can be modified at any time and in any order. 
However, because it is ultimately backed by Arrow arrays, the mutation process may be complex and less efficient 
than might be desired. Mutation is described in more detail below in the _Write Operations_ section.

## What's in a Table?
Both Table and MutableTable consist largely of a Schema and a collection of FieldVector objects. 

## Creating a Table


## Converting to a VectorSchemaRoot

```java
VectorSchemaRoot root = myTable.toVectorSchemaRoot();
```

## Row operations

Row operations are performed using a Cursor object. There are two types: MutableCursor and Immutable Cursor.
ImmutableCursor provides get operations by both vector name and vector position.

### Read operations
```java
ImmutableCursor c = table.immutableCursor(); 
int age101 = c.at(101) // change position directly to 101
                .get("age");
boolean ageIsNull = c.isNull();
```

### Write operations

Table supports read, update, append, and delete operations through its MutableCursor. To get a mutable cursor, 
you ask the table:
```java
MutableCursor c = table.mutableCursor();
```
MutableCursor also implements the public API for Cursor, so the accessor methods are available. For example:

```java
MutableCursor c = table.mutableCursor();
while (c.hasNext()){
    if(c.getInt("age") >= 18) {
        c.setNull("chaparone")
            .setVarChar("status", "adult");
    }
}
```


## Mutability semantics
In contrast to VectorSchemaRoot, Table's mutability semantics are straightforward. You can:
- append records
- update any row
- delete any row

at any time. Table does not currently support insertAt(index, value) operations, nor does it guarantee that row order remains consistent after update operations. 

There are two ways that updates are handled. They are done in place whenever possible. Otherwise, they are performed by deleting the original row, and appending a new copy of that row with the value changed.

Deletions (including deletions that occur in performing updates) are done virtually. The row to be deleted is marked as such, but remains in the table until compaction occurs. The number of deleted records is also available using Table#getDeletedCount(). This can be useful in determining when to compact.

### Compaction
The compaction process removes any rows marked for deletion. To perform compaction, simply call the compact() method:
```java
int deleted = oldTable.getDeletedCount(); 
Table compacted = oldTable.compact();
```
