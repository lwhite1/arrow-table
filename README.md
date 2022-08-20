# Table
 
Table is a mutable, tabular data structure backed by Arrow arrays. It is similar to VectorSchemaRoot in many ways, but has simpler mutation semantics, and lacks the other class's support for batch operations. 

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
MutableCursor also implements the public API for ImmutableCursor, so the accessor methods are available. For example:

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
The compaction process creates a new Table with any rows that were marked for deletion removed. To perform compaction, simply call the compact method:
```java
int deleted = oldTable.getDeletedCount(); 
Table compacted = oldTable.compact();
```
