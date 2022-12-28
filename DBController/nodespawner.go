package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
)

type HostPool struct {
	Hosts []string
}

const HOSTS_FILENAME = "hosts.json"
const DEPLOY_NODE_SCRIPT = "deplynode.sh"

var g_hostPool HostPool
var g_selfHostName string
var g_currDir string

func ParseHostsList() {
	data, err := ioutil.ReadFile(path.Join(g_currDir, HOSTS_FILENAME))
	if err != nil {
		log.Fatalf("ParseHostsList: Failed to read hosts file %s\n", path.Join(g_currDir, HOSTS_FILENAME))
	}

	err = json.Unmarshal(data, &g_hostPool)
	if err != nil {
		log.Fatalf("ParseHostsList: Failed to decode hosts file data to HostPool struct")
	}

	out, err := exec.Command("hostname").Output()
	if err != nil {
		log.Fatalf("ParseHostsList: Failed to get own hostname")
	}

	g_selfHostName = strings.TrimSuffix(string(out), "\n")
	log.Println("Self hostname:", g_selfHostName)
}

func DeployNode(hostAddr string, port int, id int) bool {
	log.Println("===== Deploying Node =====")
	log.Printf("Node addr: %s\n\t\tNode port: %d\n\t\tNode id: %d\n", hostAddr, port, id)
	cmd := exec.Command(path.Join(g_currDir, DEPLOY_NODE_SCRIPT), hostAddr, fmt.Sprintf("%d", id), fmt.Sprintf("%d", port), fmt.Sprintf("\"http://%s.utm.utoronto.ca:%d\"", g_selfHostName, g_listenPort))
	err := cmd.Start()
	if err != nil {
		log.Printf("Failed to deploy node with error: %s\n", err.Error())
		return false
	}

	err = cmd.Wait()
	if err != nil {
		log.Printf("Failed to deploy node with error: %s\n", err.Error())
		return false
	}

	log.Println("===== Finished Deploying Node =====")
	return true
}

func SetWorkingDirectory() {
	executablePath, _ := os.Executable()
	g_currDir = filepath.Dir(executablePath)
}
