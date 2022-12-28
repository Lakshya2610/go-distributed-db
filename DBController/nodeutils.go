package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type DBEntry struct {
	Key   string
	Value string
}

type DBChunk struct {
	Entries []DBEntry
	Owner   uint32 // owner node id
}

type DBEntryInfo struct {
	Entry      DBEntry
	OwnerNodes []uint32
}

const CATCHUP_NOTI_RETRIES = 3

func (network *DBNetwork) OnUpdated() {
	for i := 0; i < int(g_network.NumNodes); i++ {
		go g_network.Nodes[i].NotifyNetworkUpdated()
	}
}

func (network *DBNetwork) RemoveNode(node *DBNode) {
	g_networkLock.Lock()
	defer g_networkLock.Unlock()

	if len(network.Nodes) == 1 {
		network.Nodes = make([]DBNode, 0)
		network.NumNodes--
		network.OnUpdated()
		return
	}

	deleteIndex := -1
	for i := 0; i < int(network.NumNodes); i++ {
		if network.Nodes[i].ID == node.ID {
			deleteIndex = i
			break
		}
	}

	for i := deleteIndex; i < len(network.Nodes)-1; i++ {
		network.Nodes[i], network.Nodes[i+1] = network.Nodes[i+1], network.Nodes[i]
		network.Nodes[i].ID--
	}

	network.Nodes = network.Nodes[:len(network.Nodes)-1]
	network.NumNodes--

	log.Printf("Network: %+v\n", network)
	network.OnUpdated()
}

func HealthCheckAndAdd(node DBNode) {
	log.Printf("Verifying node %+v is healthy before adding to network\n", node)

	tries := 0
	for {
		if node.UpdateStateAndReturn() == NODESTATE_READY {
			break
		}

		tries++
		if tries == NEWNODE_HEALTHCHECK_TRIES {
			log.Printf("Node %s failed health check, it won't be added to the network\n", node.Addr)
			return // if num tries has exceeded, don't add this node - something went wrong during deployment
		}

		time.Sleep(NODE_HEALTH_CHECK_INTERVAL_S * time.Second)
	}

	log.Printf("Node %s passed health check, adding it to network\n", node.Addr)

	g_networkLock.Lock()
	g_network.Nodes = append(g_network.Nodes, node)
	g_network.NumNodes++
	g_networkLock.Unlock()

	g_network.OnUpdated()
}

func (node *DBNode) SplitAddrAndPort() (string, int) {
	parsedURL, err := url.Parse(node.Addr)
	if err != nil {
		log.Printf("SplitAddrAndPort: Failed to parse node addr %s - %s\n", node.Addr, err.Error())
		return "", -1
	}

	port, err := strconv.Atoi(parsedURL.Port())

	if err != nil {
		log.Printf("SplitAddrAndPort: Failed to parse port for node addr %s - %s\n", node.Addr, err.Error())
		return "", -1
	}

	return parsedURL.Hostname(), port
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

func (node *DBNode) SpawnReplacement() {
	time.Sleep(NODE_MAX_UNREACHABLE_TIME_S * time.Second)

	if node.State == NODESTATE_UNREACHABLE {
		node.State = NODESTATE_DEAD

		log.Printf("Spawning a replacement due to node %d being unreachable", node.ID)

		addr, port := node.SplitAddrAndPort()
		spawned := DeployNode(addr, port, int(node.ID))

		if spawned {
			node.State = NODESTATE_STARTING
			go node.Catchup()
		} else {
			g_network.RemoveNode(node)
		}
	}
}

func (node *DBNode) Catchup() {
	for i := 0; i < CATCHUP_NOTI_RETRIES; i++ {
		time.Sleep(NODE_HEALTH_CHECK_INTERVAL_S * time.Second) // let it come back online before telling it to catchup

		_, err := http.Post(node.Addr+"/internal/catchup", "text/html", http.NoBody)
		if err != nil {
			log.Printf("Failed to notify node %s to catchup | try %d\n", node.Addr, i)
		} else {
			break
		}
	}
}

func (node *DBNode) UpdateState() {
	response, err := http.Get(node.Addr + "/internal/healthcheck")
	if err != nil || response.StatusCode != http.StatusOK {
		if err == nil {
			err = errors.New("HealthCheckError: Incorrect response code")
		}
		log.Printf("Node %d: Health check failed - %s", node.ID, err.Error())
		node.State = NODESTATE_UNREACHABLE

		go node.SpawnReplacement()
	} else {
		node.State = NODESTATE_READY
	}
}

func (node *DBNode) UpdateStateAndReturn() DBNodeState {
	response, err := http.Get(node.Addr + "/internal/healthcheck")
	if err != nil || response.StatusCode != http.StatusOK {
		if err == nil {
			err = errors.New("HealthCheckError: Incorrect response code")
		}
		log.Printf("Node %d: Health check failed - %s", node.ID, err.Error())
		node.State = NODESTATE_UNREACHABLE
	} else {
		node.State = NODESTATE_READY
	}

	return node.State
}

func (node *DBNode) NotifyNetworkUpdated() {
	_, err := http.Get(node.Addr + "/internal/networkupdate")
	if err != nil {
		log.Printf("Failed to notify node of network change: %s\n", err.Error())
	}
}

func ProcessDBChunks(chunks []*DBChunk) map[string]DBEntryInfo {
	entryTable := make(map[string]DBEntryInfo)

	for _, chunk := range chunks {
		if chunk == nil {
			continue
		}
		for _, entry := range chunk.Entries {
			if value, found := entryTable[entry.Value]; found {
				owners := append(value.OwnerNodes, chunk.Owner)
				value.OwnerNodes = owners
				entryTable[entry.Value] = value
			} else {
				ownerArr := make([]uint32, 0)
				ownerArr = append(ownerArr, chunk.Owner)
				entryTable[entry.Value] = DBEntryInfo{Entry: entry, OwnerNodes: ownerArr}
			}
		}
	}

	return entryTable
}
