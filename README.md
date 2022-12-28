# go-distributed-db
A distributed key value store written in Go

- **DBController**: Node manager that controls and manages all storage nodes
- **DBNode**: A storage node. All storage nodes can communicate with each other internally, so client can submit queries to any of the
          nodes (Read or Write)
    1. The storage nodes use sqlite as the data storage backend

Note: This was initially written as a store for a URL shortner
