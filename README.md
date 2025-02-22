CSCI-421: Database System Implementation
-
Group 11
- Cameron Fanning
- D. William Campman
- Greg Lynskey
- Aayan Sayed
- Roshan Nunna

To compile: take the src folder, paste it into your desired directory, and in the ROOT of your directory (i.e. OUTSIDE OF THE SRC FOLDER), do the following cmd:
```javac **/**/*.java```

To run, go **INTO THE SRC FOLDER** and do the following:
```java Main database page_size buffer_size```

ex: java Main test1 50 5

Project Structure:
- parsers: DML and DDL take in commands, and format them to send to the Database Engine
- database engine: The database engine takes in the commands and sends appropriate calls to the Storage Manager. The database engine will also filter through whole tables for specific data in future phases.
- storage manager: Manages the in-memory buffer and physical file storage.

There are also classes for Page, Record, Attribute, Attribute Type, Catalog, and TableSchema.
