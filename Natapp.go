package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"math/rand"
	"strconv"
	"strings"
)

const SEPERATOR = ";"

var session *gocql.Session
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var n = 10

type TupleV struct {
	sourceIp        string
	sourcePort      int
	destinationIp   string
	destinationPort int
	protocol        int
}

type IPAndPort struct {
	destinationIp   string
	destinationPort int
	used            bool
}

func init() {
	cluster := gocql.NewCluster("128.52.179.161", "128.52.179.162", "128.52.179.163", "128.52.179.164", "128.52.179.165")
	cluster.Consistency = gocql.Any
	cluster.Keyspace = "NatApp"
	if session, err := cluster.CreateSession(); err != nil {
		fmt.Println(err)
		fmt.Println(session)
	}
}

/*
For this application we have two tables "ports" and "usedports". "ports" contains the list of available ports on different ips,
and also keeps track if the ports have been used or not. "usedports" keeps track of the ports that are being used
 */
func processPacket(tupleV TupleV) TupleV {
	var iPAndPort IPAndPort
	tupleVKey := createVTupleString(tupleV)
	if iPAndPort,isIn := readFromUsedPorts(tupleVKey, session); !isIn{
	//if (IPAndPort{} == iPAndPort) {
		iPAndPort = selectIPAndPort()
		updateIpsAndPorts(iPAndPort)
		updateUsedPorts(tupleVKey, iPAndPort)

		reverseTupleV := extractReverseTuple(tupleV, iPAndPort)
		sourceIpAndPort := IPAndPort{destinationIp: tupleV.sourceIp, destinationPort: tupleV.sourcePort}
		updateUsedPorts(createVTupleString(reverseTupleV),sourceIpAndPort)
	}
	tupleV.destinationIp = iPAndPort.destinationIp
	tupleV.destinationPort = iPAndPort.destinationPort
	return tupleV
}

func createVTupleString(tupleV TupleV) string{
	res := strings.Join([]string{tupleV.sourceIp, strconv.Itoa(tupleV.sourcePort), tupleV.destinationIp, strconv.Itoa(tupleV.destinationPort), strconv.Itoa(tupleV.protocol)}, SEPERATOR)
	return res
}

func readFromUsedPorts(tuple5Key string, session *gocql.Session) (IPAndPort,bool) {
	var flag bool
	ipAndPort := IPAndPort{destinationIp: "", destinationPort: 0, used : true}
	arg := fmt.Sprintf("SELECT ip,port FROM usedports WHERE key=?")
	if err := session.Query(arg, tuple5Key).Scan(&ipAndPort.destinationIp, &ipAndPort.destinationPort); err != nil {
		fmt.Println(err)
		flag = false
	} else {
		flag = ipAndPort.destinationIp == ""
	}
	return ipAndPort, flag
}

func selectIPAndPort() IPAndPort {
	var ipAndPort IPAndPort
	var ipAndPortStr string
	var flag= false

	iter := session.Query("SELECT * FROM ports").Iter()
	for {
		row := map[string]interface{}{
			"key":  &ipAndPortStr,
			"used": &ipAndPort.used,
		}
		if !iter.MapScan(row) {
			break
		} else if !ipAndPort.used {
			ipAndPortArr := strings.Split(ipAndPortStr, SEPERATOR)
			ipAndPort.destinationIp = ipAndPortArr[0]
			ipAndPort.destinationPort, _ = strconv.Atoi(ipAndPortArr[1])
			flag = true
			break
		}
	}

	if !flag {
		panic("There is no remaning ip ports")
	}
	return ipAndPort
}

//func selectIPAndPort() IPAndPort {
//	ipsAndPorts := getIPsAndPorts()
//	if len(ipsAndPorts) == 0 {
//		panic("There is no remaning ip ports")
//	}
//	index := rand.Int() % len(ipsAndPorts)
//
//	ipsAndPorts[index].used = true
//	return ipsAndPorts[index]
//}
//
//func getIPsAndPorts() []IPAndPort {
//	var ipsAndPorts []IPAndPort
//	var ipAndPortStr string
//	var ipUsed bool
//	var i = 0
//	iter := session.Query("SELECT * FROM ports").Iter()
//	for {
//		i++
//		row := map[string]interface{}{
//			"key":   &ipAndPortStr,
//			"used": &ipUsed,
//		}
//		if !iter.MapScan(row) {
//			break
//		}
//		ipAndPortArr :=strings.Split(ipAndPortStr,SEPERATOR)
//		ip := ipAndPortArr[0]
//		port,_ := strconv.Atoi(ipAndPortArr[1])
//		if !ipUsed {
//			ipAndPort := IPAndPort{destinationIp: ip, destinationPort: port, used: ipUsed}
//			ipsAndPorts[i] = ipAndPort
//		}
//	}
//	return ipsAndPorts
//}


func updateIpsAndPorts(ipAndPort IPAndPort) {
	ipAndPortKey := createIpAndPortKey(ipAndPort)
	arg := fmt.Sprintf("INSERT INTO ports (key,used) VALUES (?, ?)")
	if err := session.Query(arg, ipAndPortKey, ipAndPort.used).Exec(); err != nil {
		log.Fatal(err)
	}
}

func createIpAndPortKey(ipAndPort IPAndPort)string{
	return strings.Join([]string{ipAndPort.destinationIp, strconv.Itoa(ipAndPort.destinationPort)},SEPERATOR)
}

func updateUsedPorts(tupleVKey string, ipAndPort IPAndPort) {
	arg := fmt.Sprintf("INSERT INTO usedports (key,ip,port) VALUES (?,?,?)")
	if err := session.Query(arg, tupleVKey, ipAndPort.destinationIp, ipAndPort.destinationPort).Exec(); err != nil {
		log.Fatal(err)
	}
}

func extractReverseTuple(tupleV TupleV, ipAndPort IPAndPort) TupleV {
	reverseTuple := TupleV{sourceIp: ipAndPort.destinationIp, sourcePort: ipAndPort.destinationPort, destinationIp: tupleV.sourceIp, destinationPort: tupleV.sourcePort,protocol: tupleV.protocol}
	return reverseTuple
}

func randSeq() string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// Here we will test our app
func main(){
	numConnections := 100
	//The code is unprotected against the extremely unlikely case that a combination of ip and port will randomly repeat
	//Populating database
	for i := 0; i < numConnections; i++ {
		ipAndPort := IPAndPort{destinationIp:randSeq(),destinationPort:rand.Int(),used:false}
		fmt.Println("Ip: {}, Port : {}", ipAndPort.destinationIp,ipAndPort.destinationPort)
		updateIpsAndPorts(ipAndPort)
	}
	//Creating new 5-tuple
	for i := 0; i < numConnections; i++ {
		tupleV := TupleV{sourceIp: randSeq(),sourcePort:rand.Int(),destinationIp:randSeq(),destinationPort:rand.Int(),protocol:6}
		fmt.Println(createVTupleString(tupleV))

		newTupleV := processPacket(tupleV)
		fmt.Println(createVTupleString(newTupleV))
	}
}

