WOO README!

Team Members

- Cameron Fanning
- D. William Campman
- Greg Lynskey
- Aayan Sayed
- Roshan Nunna

To run:
java src.Main \<database path> \<page size> \<buffer size>
ex: java src.Main test_databases/test1 50 5

Project Structure:
- parsers: DML and DDL take in commands, and format them to send to the Database Engine
- database engine: The database engine takes in the commands and sends appropriate calls to the Storage Manager. The database engine will also filter through whole tables for specific data in future phases.
- storage manager: Manages the in-memory buffer and physical file storage.

There are also classes for Page, Record, Attribute, Attribute Type, Catalog, and TableSchema.
