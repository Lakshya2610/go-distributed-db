package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type DBNodeState uint8

const (
	NODESTATE_STARTING    DBNodeState = iota
	NODESTATE_READY       DBNodeState = iota
	NODESTATE_UNREACHABLE DBNodeState = iota
	NODESTATE_DEAD        DBNodeState = iota
)

const NODE_MAX_UNREACHABLE_TIME_S = 16
const NODE_HEALTH_CHECK_INTERVAL_S = 5
const NEWNODE_HEALTHCHECK_TRIES = 3

type DBNode struct {
	Addr  string      // http addr
	ID    int32       // index of the node
	State DBNodeState // last known state of this node (updates periodically)
}

type DBNetwork struct {
	Nodes             []DBNode // list of nodes
	NumNodes          uint32
	ReplicationFactor uint32
}

var g_network DBNetwork
var g_networkLock sync.Mutex
var g_replicationFactor uint
var g_minNumNodes uint
var g_listenPort uint
var g_debugLocal bool

func init() {
	flag.UintVar(&g_replicationFactor, "rf", 1, "Number of nodes that should replicate a piece of data")
	flag.UintVar(&g_minNumNodes, "n", 1, "Number of nodes to deploy in the network, replication factor should be <= than this number")
	flag.UintVar(&g_listenPort, "port", 8080, "Port that this node will bind to and listen")
	flag.BoolVar(&g_debugLocal, "debuglocal", false, "Set this flag to when running all nodes and controller locally")
}

func SetupLogger() {
	os.Remove("out.log")
	file, err := os.OpenFile("out.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Println("Failed to open logfile, logs won't be saved")
	}
	log.SetOutput(file)
}

func EnableCors(response http.ResponseWriter) {
	response.Header().Set("Access-Control-Allow-Origin", "*")
	response.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization")
	response.Header().Set("Access-Control-Allow-Credentials", "true")
	response.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH")
}

func HandlePreflightRequests(response http.ResponseWriter, request *http.Request) bool {
	if request.Method == http.MethodOptions {
		response.WriteHeader(http.StatusOK)
		return true
	}

	return false
}

func HandleNetworkInfoRequest(response http.ResponseWriter, request *http.Request) {
	EnableCors(response)

	if request.Method != http.MethodGet {
		log.Printf("[%s]: Got a request for /network route with non-get method\n", request.RemoteAddr)
		http.Error(response, "Incorrect method for route", http.StatusMethodNotAllowed)
		return
	}

	serializedNetwork, err := json.Marshal(g_network)
	if err != nil {
		log.Println("Failed to serialize network to send to node")
		http.Error(response, "Something went wrong", http.StatusInternalServerError)
		return
	}

	response.Write(serializedNetwork)
}

func HandleGetData(response http.ResponseWriter, request *http.Request) {
	EnableCors(response)

	chunks := make([]*DBChunk, 0)
	chunkLoadChan := make(chan *DBChunk)

	go FetchDataFromAllNodes(chunkLoadChan)

	remainingChunks := g_network.NumNodes

	for remainingChunks > 0 {
		nextChunk := <-chunkLoadChan
		chunks = append(chunks, nextChunk)
		remainingChunks--
	}

	respStr := `<table>
				<th>Value</th>
				<th>Key</th>
				<th>Owners</th>
				<tr></tr>`

	entryInfoTable := ProcessDBChunks(chunks)

	nodeStats := make([]int32, g_network.NumNodes)

	for _, entry := range entryInfoTable {
		respStr += fmt.Sprintf(`
						<tr>
						<td>%s</td>
						<td>%s</td>
						<td>%v</td>
						</tr>`, entry.Entry.Value, entry.Entry.Key, entry.OwnerNodes)

		for _, owner := range entry.OwnerNodes {
			nodeStats[owner]++
		}
	}

	respStr += "</table><br>"

	for i := 0; i < int(g_network.NumNodes); i++ {
		respStr += fmt.Sprintf("<span>Node %d: %d entries</span><br>", i, nodeStats[i])
	}

	io.WriteString(response, respStr)
}

func FetchDataFromAllNodes(writeChannel chan<- *DBChunk) {
	for _, node := range g_network.Nodes {

		go func(node DBNode) {
			data := node.GetAllData()
			writeChannel <- data
		}(node)

	}
}

func MonitorNodes() {
	for {
		time.Sleep(NODE_HEALTH_CHECK_INTERVAL_S * time.Second)
		g_networkLock.Lock()

		for i := 0; i < len(g_network.Nodes); i++ {
			node := &g_network.Nodes[i]
			if node.State == NODESTATE_DEAD {
				continue // node is dead, don't ping it
			}

			node.UpdateState()
		}

		g_networkLock.Unlock()
	}
}

func HandleAddNode(response http.ResponseWriter, request *http.Request) {
	EnableCors(response)

	if request.Method != http.MethodPost {
		log.Printf("[%s]: Got a request for /addnode route with non-post method\n", request.RemoteAddr)
		http.Error(response, "Incorrect method for route", http.StatusMethodNotAllowed)
		return
	}

	query := request.URL.Query()
	nodeURL := query.Get("nodeurl")
	if len(nodeURL) == 0 {
		log.Printf("[%s]: invalid query params for /addnode", request.RemoteAddr)
		http.Error(response, "Invalid params", http.StatusBadRequest)
		return
	}

	node := DBNode{ID: int32(g_network.NumNodes), Addr: nodeURL}
	addr, port := node.SplitAddrAndPort()
	spawned := DeployNode(addr, port, int(node.ID))

	if spawned {
		go HealthCheckAndAdd(node)
	}

	response.WriteHeader(http.StatusCreated)
}

func HandleRFUpdate(response http.ResponseWriter, request *http.Request) {
	EnableCors(response)
	if HandlePreflightRequests(response, request) {
		return
	}

	if request.Method != http.MethodPatch {
		log.Printf("[%s]: Got a request for /rfupdate route with non-patch method\n", request.RemoteAddr)
		http.Error(response, "Incorrect method for route", http.StatusMethodNotAllowed)
		return
	}

	query := request.URL.Query()
	newReplicationFactor := query.Get("rf")
	if len(newReplicationFactor) == 0 {
		log.Printf("[%s]: invalid query params for /rfupdate", request.RemoteAddr)
		http.Error(response, "Invalid params", http.StatusBadRequest)
		return
	}

	newRF, err := strconv.Atoi(newReplicationFactor)
	if err != nil {
		log.Printf("[%s]: failed to parse new RF for /rfupdate", request.RemoteAddr)
		http.Error(response, "Invalid params", http.StatusBadRequest)
		return
	}

	if newRF != int(g_network.ReplicationFactor) && newRF > 0 && newRF <= int(g_network.NumNodes) {
		log.Printf("Changing replication factor from %d to %d\n", g_network.ReplicationFactor, newRF)

		g_network.ReplicationFactor = uint32(newRF)
		g_network.OnUpdated()

		response.WriteHeader(http.StatusNoContent)
		return
	} else {
		log.Printf("Got same/invalid replication factor '%d', so it will be unchanged\n", newRF)
		response.WriteHeader(http.StatusNotAcceptable)
		io.WriteString(response, "Replication factor provided is either the same/invalid, so not changes were made")
		return
	}
}

func HandleKillNode(response http.ResponseWriter, request *http.Request) {
	EnableCors(response)
	if HandlePreflightRequests(response, request) {
		return
	}

	if request.Method != http.MethodPatch {
		log.Printf("[%s]: Got a request for /killnode route with non-patch method\n", request.RemoteAddr)
		http.Error(response, "Incorrect method for route", http.StatusMethodNotAllowed)
		return
	}

	query := request.URL.Query()
	nodeIDString := query.Get("nodeID")
	if len(nodeIDString) == 0 {
		log.Printf("[%s]: invalid query params for /killnode", request.RemoteAddr)
		http.Error(response, "Invalid params", http.StatusBadRequest)
		return
	}

	nodeID, err := strconv.Atoi(nodeIDString)
	if err != nil {
		log.Printf("[%s]: failed to parse nodeID for /killnode", request.RemoteAddr)
		http.Error(response, "Invalid params", http.StatusBadRequest)
		return
	}

	for i := 0; i < len(g_network.Nodes); i++ {
		node := &g_network.Nodes[i]
		if node.ID == int32(nodeID) {
			go g_network.RemoveNode(node)
			break
		}
	}

	response.WriteHeader(http.StatusNoContent)
}

func Debug_SetupNodes() {
	nodePort := 5000
	for i := 0; i < int(g_minNumNodes); i++ {
		node := DBNode{ID: int32(i), Addr: fmt.Sprintf("http://localhost:%d", nodePort)}
		g_network.Nodes = append(g_network.Nodes, node)
		nodePort++
	}
}

func StartServer(serverExitNotifier chan<- bool) {
	http.HandleFunc("/network", HandleNetworkInfoRequest) // GET
	http.HandleFunc("/data", HandleGetData)               // GET
	http.HandleFunc("/addnode", HandleAddNode)            // POST
	http.HandleFunc("/killnode", HandleKillNode)          // PATCH
	http.HandleFunc("/rfupdate", HandleRFUpdate)          // PATCH
	http.ListenAndServe(fmt.Sprintf(":%d", g_listenPort), nil)
	serverExitNotifier <- true
}

func main() {
	flag.Parse()
	SetWorkingDirectory()

	if g_replicationFactor > g_minNumNodes {
		log.Fatalln("Invalid config. Replication factor less than minimum number of nodes")
	}

	ParseHostsList()
	if !g_debugLocal && len(g_hostPool.Hosts) < int(g_minNumNodes) {
		log.Fatalln("Invalid config, number of hosts in pool less than min number of nodes")
	}

	SetupLogger()
	serverExitNotifier := make(chan bool)

	if g_debugLocal {
		Debug_SetupNodes()
		go StartServer(serverExitNotifier)
	} else {
		const nodePort = 5000
		hostAddresses := make([]string, 0)
		for i := 0; i < int(g_minNumNodes); i++ {
			node := DBNode{ID: int32(i), Addr: fmt.Sprintf("http://%s:%d", g_hostPool.Hosts[i], nodePort)}
			hostAddresses = append(hostAddresses, g_hostPool.Hosts[i])
			g_network.Nodes = append(g_network.Nodes, node)
		}

		go StartServer(serverExitNotifier)

		time.Sleep(1 * time.Second) // let the http server startup
		for index, node := range g_network.Nodes {
			log.Printf("Deploying node: %v\n", node)
			if !DeployNode(hostAddresses[index], nodePort, int(node.ID)) {
				log.Fatalln("Failed to deploy all nodes during initialization")
			}
		}
	}

	g_network.NumNodes = uint32(len(g_network.Nodes))
	g_network.ReplicationFactor = uint32(g_replicationFactor)
	log.Printf("Network:\n%+v\n", g_network)

	go MonitorNodes()

	<-serverExitNotifier
}
