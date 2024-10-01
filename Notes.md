# 30 Sept 2024

- Staring this project called Goat DB
- At this point I want to create a new no SQL DB that is optimized for writes and works well in a single node
- Using the folowing DBs for reference: badger, junodb and dolt.
- DB is an abstraction on Txn
- The Open function in DB is long running function that keeps running and writes data to disk
- Try creating a simple DB in golang that is capable of reading\writing data in-memory tomorrow

# 1 Oct 2024

- Create db.go with two operations Get and Put that allow adding and retrieving a KV pair
