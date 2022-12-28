package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"
)

const SEND_RETRY_INTERVAL_S = 2

func (node *DBNode) Send(data DBEntry, numTries uint16) {
	for numTries > 0 {
		_, err := http.Post(fmt.Sprintf("%s/internal/set?key=%s&value=%s", node.Addr, url.QueryEscape(data.Key), url.QueryEscape(data.Value)), "application/json", http.NoBody)
		if err != nil {
			log.Printf("Failed to send data to node %v: %s", node, err.Error())
		} else {
			return
		}

		numTries--
		if numTries > 0 {
			time.Sleep(SEND_RETRY_INTERVAL_S * time.Second)
		}
	}
}

func (node *DBNode) GetAllData() *DBChunk {
	res, err := http.Get(node.Addr + "/internal/getall")
	if err != nil {
		log.Printf("failed to fetch data from node %d: %s", node.ID, err.Error())
		return nil
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Printf("Failed to parse data sent by node %d: %s", node.ID, err.Error())
		return nil
	}

	var data DBChunk
	err = json.Unmarshal(body, &data)
	if err != nil {
		log.Printf("Failed to parse data sent by node %d into DBChunk: %s", node.ID, err.Error())
		return nil
	}

	return &data
}

func (node *DBNode) SendChunk(data *DBChunk, numTries uint16) {
	serializedChunk, err := json.Marshal(data)
	log.Printf("Chunk: %s\n", string(serializedChunk))
	if err != nil {
		log.Printf("SendChunk: Failed to serialize chunk for target node %d\n", data.Owner)
		return
	}

	for numTries > 0 {
		_, err := http.Post(fmt.Sprintf("%s/internal/setchunk", node.Addr), "application/json", bytes.NewBuffer(serializedChunk))
		if err != nil {
			log.Printf("Failed to send data to node %v: %s", node, err.Error())
		} else {
			return
		}

		numTries--
		if numTries > 0 {
			time.Sleep(SEND_RETRY_INTERVAL_S * time.Second)
		}
	}
}

func GetChunkFromNode(id uint32) *DBChunk {
	for _, node := range g_dbNetwork.Nodes {
		if node.ID == int32(id) {
			chunk := node.GetAllData()
			return chunk
		}
	}

	return nil
}

func GetDataFromNode(key string, id uint32) *DBEntry {
	for _, node := range g_dbNetwork.Nodes {
		if node.ID == int32(id) {
			res, err := http.Get(fmt.Sprintf("%s/internal/get?key=%s", node.Addr, url.QueryEscape(key)))

			if err != nil {
				log.Printf("GetDataFromNode: Failed to fetch key=%s from node=%d\n", key, id)
				return nil
			}

			if res.StatusCode == http.StatusOK {
				body, err := ioutil.ReadAll(res.Body)
				if err != nil {
					log.Printf("GetDataFromNode: Failed to read body for response\n")
					return nil
				}

				var entry DBEntry
				err = json.Unmarshal(body, &entry)
				if err != nil {
					log.Printf("GetDataFromNode: Failed to parse body for response\n")
					return nil
				}

				return &entry
			}
		}
	}

	return nil
}

func SendToNodeWithID(data DBEntry, id uint32, numTries uint16) {
	for _, node := range g_dbNetwork.Nodes {
		if node.ID == int32(id) {
			node.Send(data, numTries)
			return
		}
	}
}

func SendChunkToNodeWithID(data *DBChunk, id uint32, numTries uint16) {
	for _, node := range g_dbNetwork.Nodes {
		if node.ID == int32(id) {
			node.SendChunk(data, numTries)
			return
		}
	}
}

func (node *DBNode) IsSameAs(other *DBNode) bool {
	return node.Addr == other.Addr && node.ID == other.ID
}

func (network *DBNetwork) NodeExists(node *DBNode) bool {
	for _, currNode := range network.Nodes {
		if currNode.IsSameAs(node) {
			return true
		}
	}

	return false
}

// compares local version of DBNetwork against the provided
// and returns true if any changes are detected
func DiffNetworkAgainstLocal(network *DBNetwork) bool {
	if network.NumNodes != g_dbNetwork.NumNodes || network.ReplicationFactor != g_dbNetwork.ReplicationFactor {
		return true
	}

	for _, node := range network.Nodes {
		if !g_dbNetwork.NodeExists(&node) {
			return true
		}
	}

	return false
}
