CSCI-421: Database System Implementation
-
Group 11
- Cameron Fanning
- D. William Campman
- Greg Lynskey
- Aayan Sayed
- Roshan Nunna

Project Description:
This was a 3 Phase project which implemented a relational database similar to postgres in Java. 

Phase 1: This phase was spent building out the basic functionality of the storage manager and the database engine. The storage manager was designed to read in basic SQL and store the data to disk using the buffer. 

Phase 2: This phase we built out the DML parser and added more advanced functionality to our system like SELECT, UPDATE, and DELETE.

Phase 3: This phase we optimized our database system to use B+ trees for indexing to speed up operations. 

Build Instructions: 
To compile: take the src folder, paste it into your desired directory, and in the ROOT of your directory (i.e. OUTSIDE OF THE SRC FOLDER), do the following cmd:
```javac **/**/*.java```

To run, go **INTO THE SRC FOLDER** and do the following:
```java Main <database> <page_size> <buffer_size>```

ex: `java Main test1 50 5`

To run the database with Indexing to speed up database operations:
```java Main <database> <page_size> <buffer_size> <IndexingOnOff>```

Indexing is turned off by default but can be set to true:
ex: `java Main test1 50 5 true` 


Project Structure:
- Bplus:
  - BPlusNode: Creates a BPlus Node for a given table which is a wrapper for a bunch of pointers.

  - BplusPointer: Class that represents the pointers inside each note. Made up of 2 values:
    - page pointer => next page
    - record pointer => -1 OR pointer to record
    
- Components: 
  - Buffer: Class representing the page buffer using a specified size.
  - Database Engine: Class for performing SQL actions, as directed by the parsers.
  - Storage Manager: Manages fetching and saving pages to file.

- Exceptions:
  - CustomExceptions: Exceptions used throughout the Database system to better convey information.
    
- Parsers:
  - DDL: Parser for commands which modify relational schemas
  - DML: Parser for commands which modify relational data

- Table Data:
  - Attribute: Representation of a attribute with flags set for constraints like: PRIMARYKEY, NOTNULL, and UNIQUE.
  - AttributeType: Enum used to represent the types of each attribute: Double, INT, Varchar(n).
  - Bufferable: Superclass for any object which can be stored inside the buffer. 
  - Catalog: Holds metainfo for the system. On startup grabs file if it's there. If not, creates empty catalog file.
  - Page: Represents the page of the table. 
  - Record: Represents a row of data in a table. 
  - TableSchema: Called from the storage manager to create a schema for a given table. 
 
- Where:
  - Main: On start up initializes the database system then reads in input from the user and sends to the parsers for validation.


