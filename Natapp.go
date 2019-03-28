package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"math/rand"
	"strconv"
	"strings"
)

var session *gocql.Session
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var n = 10

type Tuple5 struct {
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
	cluster := gocql.NewCluster("128.52.162.124", "128.52.162.125", "128.52.162.131", "128.52.162.127", "128.52.162.122", "128.52.162.120")
	cluster.Keyspace = "NetApp"
	session, err := cluster.CreateSession()
	if err != nil {
		fmt.Println(err)
		fmt.Println(session)
	}
}

func processPacket(tuple5 Tuple5) Tuple5 {
	tuple5Key := create5TupleKey(tuple5)
	iPAndPort := readFromUsedPorts(tuple5Key, session)
	if (IPAndPort{} == iPAndPort) {
		iPAndPort = selectIPAndPort();
		iPAndPort.used = true
		updateIpsAndPorts(iPAndPort)
		updateUsedPorts(tuple5Key, iPAndPort);
		reverseTuple5 := extractReverseTuple(tuple5, iPAndPort)
		sourceIpAndPort := IPAndPort{destinationIp: tuple5.sourceIp, destinationPort: tuple5.sourcePort}
		updateUsedPorts(create5TupleKey(reverseTuple5),sourceIpAndPort)
	}
	tuple5.destinationIp = iPAndPort.destinationIp
	tuple5.destinationPort = iPAndPort.destinationPort
	return tuple5
}

func create5TupleKey(tuple5 Tuple5) string {
	return create5TupleString(tuple5,"")
}
func create5TupleString(tuple5 Tuple5, seperator string) string{
	string := strings.Join([]string{tuple5.sourceIp, strconv.Itoa(tuple5.sourcePort), tuple5.destinationIp, strconv.Itoa(tuple5.destinationPort), strconv.Itoa(tuple5.protocol)}, seperator)
	return string
}

func readFromUsedPorts(tuple5Key string, session *gocql.Session) IPAndPort {
	var ipAndPort IPAndPort
	arg := fmt.Sprintf("SELECT ip,port FROM usedports WHERE key=?")
	if err := session.Query(arg, tuple5Key).Scan(&ipAndPort.destinationIp, &ipAndPort.destinationPort); err != nil {
		log.Fatal(err)
	}
	return ipAndPort
}

func selectIPAndPort() IPAndPort {
	ipsAndPorts := getIPsAndPorts();
	if (len(ipsAndPorts) == 0) {
		panic("There is no remaning ip ports")
	}
	index := rand.Int() % len(ipsAndPorts)

	return ipsAndPorts[index]
}

func getIPsAndPorts() []IPAndPort {
	var ipsAndPorts []IPAndPort
	var ip string
	var port int
	var ipUsed bool
	var i = 0
	iter := session.Query("SELECT * from ports").Iter()
	for {
		i++;
		row := map[string]interface{}{
			"ip":   &ip,
			"port": &port,
			"used": &ipUsed,
		}
		if !iter.MapScan(row) {
			break
		}
		if (!ipUsed) {
			ipAndPort := IPAndPort{destinationIp: ip, destinationPort: port, used: ipUsed}
			ipsAndPorts[i] = ipAndPort
		}
	}
	return ipsAndPorts
}

func updateIpsAndPorts(ipAndPort IPAndPort) {
	arg := fmt.Sprintf("INSERT INTO ports (ip,port,used) values (?, ?, ?)")
	if err := session.Query(arg, ipAndPort.destinationIp, ipAndPort.destinationPort, ipAndPort.used).Exec(); err != nil {
		log.Fatal(err)
	}
}
func updateUsedPorts(tuple5Key string, ipAndPort IPAndPort) {
	arg := fmt.Sprintf("Insert into usedports (key,ip,port)")
	if err := session.Query(arg, tuple5Key, ipAndPort.destinationIp, ipAndPort.destinationPort).Exec(); err != nil {
		log.Fatal(err)
	}
}

func extractReverseTuple(tuple5 Tuple5, ipAndPort IPAndPort) Tuple5 {
	reverseTuple := Tuple5{sourceIp: ipAndPort.destinationIp, sourcePort: ipAndPort.destinationPort, destinationIp: tuple5.sourceIp, destinationPort: tuple5.sourcePort,protocol:tuple5.protocol}
	return reverseTuple
}

func randSeq() string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
func printTuple5(tuple5 Tuple5){
	fmt.Println("%v",create5TupleString(tuple5," "));
}
//Here we will test our app
func main(){
	var numConnections = 100
	//The code is unprotected against the extremely unlikely case that a combination of ip and port will randomly repeat
	//Populating database
	for i := 0; i < numConnections; i++ {
		ipAndPort := IPAndPort{destinationIp:randSeq(),destinationPort:rand.Int(),used:false}
		fmt.Println("Ip: %v, Port : %v", ipAndPort.destinationIp,ipAndPort.destinationPort)
		updateIpsAndPorts(ipAndPort)
	}
	//Creating new 5-tuple
	for i := 0; i < numConnections; i++ {
		tuple5 := Tuple5{sourceIp:randSeq(),sourcePort:rand.Int(),destinationIp:randSeq(),destinationPort:rand.Int(),protocol:6}
		printTuple5(tuple5)
		newTuple5 :=processPacket(tuple5)
		printTuple5(newTuple5)
	}



}

