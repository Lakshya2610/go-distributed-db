package main

import (
	"hash/fnv"
	"log"
)

type DBEntry struct {
	Key   string
	Value string
}

type DBChunk struct {
	Entries []DBEntry
	Owner   uint32 // owner node id
}

const SINGLE_TRY uint16 = 1
const THREE_TRIES uint16 = 3
const CACHE_SIZE uint32 = 500

var g_dataCache LRUCache = MakeLRUCache(CACHE_SIZE)

func (entry *DBEntry) Hash() uint64 {
	hash := fnv.New64()
	hash.Write([]byte(entry.Key))
	return hash.Sum64()
}

func (entry *DBEntry) GetTargetNodes() []uint32 {
	targetNodes := make([]uint32, g_dbNetwork.ReplicationFactor)
	nodeID := uint32(entry.Hash() % uint64(g_dbNetwork.NumNodes))

	for i := 0; i < int(g_dbNetwork.ReplicationFactor); i++ {
		targetNodes[i] = nodeID
		nodeID = (nodeID + 1) % g_dbNetwork.NumNodes
	}

	return targetNodes
}

func DB_Write(data DBEntry) {
	targetNodes := data.GetTargetNodes()
	log.Printf("Entry %+v will be written to %v nodes\n", data, targetNodes)

	for _, nodeID := range targetNodes {
		if nodeID == uint32(g_id) {
			go DB_LocalWrite(data)
		} else {
			go SendToNodeWithID(data, nodeID, SINGLE_TRY)
		}
	}
}

// returns the value for the corrosponding key (if it exists)
func DB_Read(key string) *DBEntry {
	if value, found := g_dataCache.Find(key); found {
		return &value
	}

	entry := DBEntry{Key: key, Value: ""}
	ownerNodes := entry.GetTargetNodes()

	hasDataLocally := false
	for _, ownerID := range ownerNodes {
		if ownerID == uint32(g_id) {
			hasDataLocally = true
			break
		}
	}

	if hasDataLocally {
		savedEntry := Sqlite_Read(entry.Key)
		if savedEntry != nil {
			g_dataCache.Add(*savedEntry)
			return savedEntry
		}
	}

	for _, ownerID := range ownerNodes {
		if ownerID != uint32(g_id) {
			savedEntry := GetDataFromNode(key, ownerID)
			if savedEntry != nil {
				return savedEntry
			}

			log.Printf("Failed to get value from node %d, trying a different node\n", ownerID)
		}
	}

	log.Printf("Failed to find value for %s\n", key)
	return nil
}

func DB_RehashData() {
	localChunk := DB_GetLocalChunk()
	if localChunk == nil {
		log.Println("DB_RehashData: failed to rehash data because DB_GetLocalChunk returned nil")
		return
	}

	nodeToEntriesTable := make(map[uint32]*DBChunk, 0)

	for _, entry := range localChunk.Entries {
		targetNodes := entry.GetTargetNodes()

		// TODO: batch this process, when data size is large, it seems that rehash can crash nodes by overwhelming them
		shouldDeleteThisEntry := true
		for _, nodeID := range targetNodes {
			if int(nodeID) != g_id {
				if chunk, found := nodeToEntriesTable[nodeID]; found {
					chunk.Entries = append(chunk.Entries, entry)
				} else {
					nodeToEntriesTable[nodeID] = &DBChunk{Entries: make([]DBEntry, 0), Owner: nodeID}
					chunk := nodeToEntriesTable[nodeID]
					chunk.Entries = append(chunk.Entries, entry)
				}
			} else {
				shouldDeleteThisEntry = false
			}
		}

		if shouldDeleteThisEntry {
			go DB_LocalDelete(entry)
		}
	}

	for id, chunk := range nodeToEntriesTable {
		go SendChunkToNodeWithID(chunk, id, THREE_TRIES) // this is a lot of data, try a few times before giving up
	}

	g_dataCache.Purge()
}

func DB_LocalWrite(data DBEntry) bool {
	targetNodes := data.GetTargetNodes()
	shouldSaveLocally := false
	for _, id := range targetNodes {
		if id == uint32(g_id) {
			shouldSaveLocally = true
			break
		}
	}

	if !shouldSaveLocally {
		log.Printf("DB_LocalWrite: called with entry %+v but it doesn't belong on this node (id=%d)\n", data, g_id)
		return false
	}

	if _, found := g_dataCache.Find(data.Key); found {
		g_dataCache.UpdateValue(data) // if entry in cache, update it as well
	}

	success := Sqlite_Write(data)
	if success {
		log.Printf("DB_LocalWrite: Wrote %+v to database\n", data)
	}

	return true
}

func DB_LocalWriteChunk(chunk *DBChunk) {
	for _, entry := range chunk.Entries {
		DB_LocalWrite(entry)
	}
}

func DB_LocalRead(key string) *DBEntry {
	if value, found := g_dataCache.Find(key); found {
		return &value // cache hit
	}

	return Sqlite_Read(key)
}

func DB_LocalDelete(data DBEntry) {
	g_dataCache.Delete(data.Key)
	Sqlite_Delete(data.Key)
}

func DB_GetLocalChunk() *DBChunk {
	data := Sqlite_ReadAll()

	data.Owner = uint32(g_id)
	return data
}
