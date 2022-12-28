package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

type DBNodeState uint8

const (
	NODESTATE_STARTING    DBNodeState = iota
	NODESTATE_READY       DBNodeState = iota
	NODESTATE_UNREACHABLE DBNodeState = iota
	NODESTATE_DEAD        DBNodeState = iota
)

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

const INVALID_ID = -1

var g_id int
var g_listenPort uint
var g_controllerAddr string
var g_dbNetwork DBNetwork

func init() {
	flag.IntVar(&g_id, "id", -1, "ID/Index of the node")
	flag.StringVar(&g_controllerAddr, "controller", "http://localhost:8080", "Address of the database controller")
	flag.UintVar(&g_listenPort, "port", 8000, "Port that this node will bind to and listen")
}

func Mod(d, m int) int {
	var res int = d % m
	if (res < 0 && m > 0) || (res > 0 && m < 0) {
		return res + m
	}
	return res
}

func DownloadNetworkInfo() DBNetwork {
	res, err := http.Get(g_controllerAddr + "/network")
	if err != nil {
		log.Fatalf("Failed to download node list from controller: %s\n", err.Error())
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Fatalf("Failed to parse response from controller: %s\n", err.Error())
	}

	var network DBNetwork
	err = json.Unmarshal(body, &network)
	if err != nil {
		log.Fatalf("Failed to parse node list: %s\n", err.Error())
	}

	log.Printf("Successfully downloaded network info:\n%+v\n", network)
	return network
}

func SyncSelfID(updatedNetwork *DBNetwork) {
	addr := ""
	for _, node := range g_dbNetwork.Nodes {
		if node.ID == int32(g_id) {
			addr = node.Addr
			break
		}
	}

	for i := 0; i < int(updatedNetwork.NumNodes); i++ {
		if updatedNetwork.Nodes[i].Addr == addr {
			g_id = int(updatedNetwork.Nodes[i].ID) // keep self ID in sync
			break
		}
	}
}

func ValidateWriteRequest(response http.ResponseWriter, request *http.Request) bool {
	if request.Method != http.MethodPost {
		log.Printf("[%s]: Got a request for /set route with non-post method\n", request.RemoteAddr)
		http.Error(response, "Incorrect method for route", http.StatusMethodNotAllowed)
		return false
	}

	query := request.URL.Query()
	key := query.Get("key")
	value := query.Get("value")

	if len(key) == 0 || len(value) == 0 {
		log.Printf("[%s]: invalid query params for /set", request.RemoteAddr)
		http.Error(response, "Invalid params", http.StatusBadRequest)
		return false
	}

	return true
}

func ProcessWrite(response http.ResponseWriter, request *http.Request) {
	if !ValidateWriteRequest(response, request) {
		return
	}

	query := request.URL.Query()
	key := query.Get("key")
	value := query.Get("value")

	log.Printf("[%s]:Got a post request for /set with key=%s, value=%s\n", request.RemoteAddr, key, value)

	DB_Write(DBEntry{Key: key, Value: value})

	response.WriteHeader(http.StatusCreated)
	io.WriteString(response, GetApproxWriteDelay())
}

// Try Write but don't propogate to other nodes
func ProcessSingleWrite(response http.ResponseWriter, request *http.Request) {
	if !ValidateWriteRequest(response, request) {
		return
	}

	query := request.URL.Query()
	key := query.Get("key")
	value := query.Get("value")

	log.Printf("[%s]:Got a post request for /internal/set with key=%s, value=%s\n", request.RemoteAddr, key, value)

	success := DB_LocalWrite(DBEntry{Key: key, Value: value})

	if success {
		response.WriteHeader(http.StatusCreated)
		io.WriteString(response, "Success")
	} else {
		response.WriteHeader(http.StatusNotAcceptable)
	}
}

func HandleGetAllData(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		log.Printf("[%s]: Got a request for /internal/getall route with non-get method\n", request.RemoteAddr)
		http.Error(response, "Incorrect method for route", http.StatusMethodNotAllowed)
		return
	}

	chunk, err := json.Marshal(DB_GetLocalChunk())
	if err != nil {
		log.Println("HandleGetAllData: Failed to serialize local data chunk", err.Error())
		http.Error(response, "Error serializing local chunk", http.StatusInternalServerError)
		return
	}

	response.Write(chunk)
}

func HandleHealthCheck(response http.ResponseWriter, request *http.Request) {
	response.WriteHeader(http.StatusOK)
}

func HandleNetworkUpdate(response http.ResponseWriter, request *http.Request) {
	log.Println("/internal/networkupdate: Network update received")

	updatedNetwork := DownloadNetworkInfo()
	if DiffNetworkAgainstLocal(&updatedNetwork) {
		log.Printf("Network changed, rehashing data")

		SyncSelfID(&updatedNetwork)
		g_dbNetwork = updatedNetwork
		DB_RehashData()
	} else {
		log.Printf("/internal/networkupdate: No change detected\n curr: %v\n new: %v\n", g_dbNetwork, updatedNetwork)
	}
}

func ValidateGetRequest(response http.ResponseWriter, request *http.Request) bool {
	if request.Method != http.MethodGet {
		log.Printf("[%s]: Got a request for /get route with non-get method\n", request.RemoteAddr)
		http.Error(response, "Incorrect method for route", http.StatusMethodNotAllowed)
		return false
	}

	query := request.URL.Query()
	key := query.Get("key")
	if len(key) == 0 {
		log.Printf("[%s]: Got a request for /get route with invalid param\n", request.RemoteAddr)
		http.Error(response, "Incorrect method for route", http.StatusBadRequest)
		return false
	}

	return true
}

func HandleGet(response http.ResponseWriter, request *http.Request) {
	if !ValidateGetRequest(response, request) {
		return
	}

	query := request.URL.Query()
	key := query.Get("key")

	log.Printf("[%s]: Got a request for /get route for key=%s\n", request.RemoteAddr, key)

	entry := DB_Read(key)
	if entry == nil {
		log.Printf("[%s]: failed to find value for key=%s\n", request.RemoteAddr, key)
		http.Error(response, "Failed to find original URL, is the key correct?", http.StatusNotFound)
		return
	}

	io.WriteString(response, entry.Value)
}

func HandleInternalGet(response http.ResponseWriter, request *http.Request) {
	if !ValidateGetRequest(response, request) {
		return
	}

	query := request.URL.Query()
	key := query.Get("key")

	log.Printf("[%s]: Got a request for /internal/get route for key=%s\n", request.RemoteAddr, key)

	entry := DB_LocalRead(key)
	if entry == nil {
		log.Printf("[%s]: Failed to find value locally for key=%s\n", request.RemoteAddr, key)
		http.Error(response, "Not found", http.StatusNotFound)
		return
	}

	body, err := json.Marshal(*entry)
	if err != nil {
		log.Printf("[%s]: Failed to serialize DBEntry for key=%s\n", request.RemoteAddr, key)
		http.Error(response, "Internal Error", http.StatusInternalServerError)
		return
	}

	response.Write(body)
}

func HandleSetChunk(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		log.Printf("[%s]: Got a request for /internal/setchunk route with non-post method\n", request.RemoteAddr)
		http.Error(response, "Incorrect method for route", http.StatusMethodNotAllowed)
		return
	}

	log.Printf("[%s]: Got a request for /internal/setchunk route\n", request.RemoteAddr)

	body, err := ioutil.ReadAll(request.Body)

	if err != nil {
		log.Printf("[%s]: Failed to parse body of request for /internal/setchunk\n", request.RemoteAddr)
		http.Error(response, "Internal Error", http.StatusInternalServerError)
		return
	}

	var chunk DBChunk
	err = json.Unmarshal(body, &chunk)
	if err != nil {
		log.Printf("[%s]: Failed to de-serialize DBChunk /internal/setchunk\n", request.RemoteAddr)
		http.Error(response, "Internal Error", http.StatusInternalServerError)
		return
	}

	if chunk.Owner != uint32(g_id) {
		log.Printf("Got chunk with owner ID %d, but self id is %d\n", chunk.Owner, g_id)
		http.Error(response, "Got chunk with owner ID not meant for me", http.StatusNotAcceptable)
		return
	}

	log.Printf("Got chunk with %d entries to save\n", len(chunk.Entries))
	go DB_LocalWriteChunk(&chunk)

	response.WriteHeader(http.StatusCreated)
}

// controller told us to catch up to other nodes, download data from nodes that might be missing here
func HandleCatchupCmd(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		log.Printf("[%s]: Got a request for /internal/catchup route with non-post method\n", request.RemoteAddr)
		http.Error(response, "Incorrect method for route", http.StatusMethodNotAllowed)
		return
	}

	log.Println("Got request for /internal/catchup, starting catchup process")

	updatedNetwork := DownloadNetworkInfo()
	if DiffNetworkAgainstLocal(&updatedNetwork) {
		log.Printf("Network changed, rehashing data")

		SyncSelfID(&updatedNetwork)
		g_dbNetwork = updatedNetwork
	}

	nodesToAsk := make([]int, 0)
	lookAheadWindow := g_dbNetwork.ReplicationFactor - 1

	for i := 0; i < int(lookAheadWindow); i++ {
		nodesToAsk = append(nodesToAsk, (i+g_id+1)%int(g_dbNetwork.NumNodes))
		nodesToAsk = append(nodesToAsk, Mod((g_id-1-i), int(g_dbNetwork.NumNodes)))
	}

	log.Printf("Asking nodes %v for their chunks for catchup process\n", nodesToAsk)

	for _, nodeID := range nodesToAsk {
		chunk := GetChunkFromNode(uint32(nodeID))
		if chunk != nil {
			go DB_LocalWriteChunk(chunk)
		}
	}
}

func SetupLogger() {
	file, err := os.OpenFile(fmt.Sprintf("out-%d.log", g_id), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Println("Failed to open logfile, logs won't be saved")
	}
	log.SetOutput(file)
	log.Println("====== Starting Logs ======")
}

func main() {
	flag.Parse()
	SetupLogger()

	if g_id <= INVALID_ID {
		log.Fatalln("Invalid id provided for node, exiting")
	}

	log.Printf("Running with ID: %d, port: %d, pid: %d\n", g_id, g_listenPort, os.Getpid())

	go func() {
		g_dbNetwork = DownloadNetworkInfo()
	}()

	go Sqlite_Connect()

	http.HandleFunc("/set", ProcessWrite)
	http.HandleFunc("/get", HandleGet)
	http.HandleFunc("/internal/set", ProcessSingleWrite)
	http.HandleFunc("/internal/getall", HandleGetAllData)
	http.HandleFunc("/internal/healthcheck", HandleHealthCheck)
	http.HandleFunc("/internal/networkupdate", HandleNetworkUpdate)
	http.HandleFunc("/internal/get", HandleInternalGet)
	http.HandleFunc("/internal/setchunk", HandleSetChunk)
	http.HandleFunc("/internal/catchup", HandleCatchupCmd)

	http.ListenAndServe(fmt.Sprintf(":%d", g_listenPort), nil)
}
