// +build new

package gototo

import (
	"encoding/json"
	zmq "github.com/JeremyOT/gozmq"
	"log"
	"runtime"
)

var logger *log.Logger

func SetLogger(l *log.Logger) {
	logger = l
}

func writeLog(message ...interface{}) {
	if logger != nil {
		logger.Println(message)
	} else {
		println(message)
	}
}

var registeredWorkerFunctions map[string]WorkerFunction = make(map[string]WorkerFunction)
var activeTimeout = 1
var passiveTimeout = 100

type WorkerFunction func(interface{}) interface{}

func RunRouter(routerAddress, dealerAddress string, routerBind, dealerBind bool) error {
	context, _ := zmq.NewContext()
	defer context.Close()
	router, _ := context.NewSocket(zmq.ROUTER)
	defer router.Close()
	if routerBind {
		router.Bind(routerAddress)
	} else {
		router.Connect(routerAddress)
	}
	dealer, _ := context.NewSocket(zmq.DEALER)
	defer dealer.Close()
	if dealerBind {
		dealer.Bind(dealerAddress)
	} else {
		dealer.Connect(dealerAddress)
	}
	return zmq.Device(zmq.QUEUE, router, dealer)
}

func RegisterWorkerFunction(name string, workerFunction WorkerFunction) {
	registeredWorkerFunctions[name] = workerFunction
}

func callworker(responseChannel chan [][]byte, message [][]byte, parameters interface{}, workerFunction WorkerFunction) {
	response := workerFunction(parameters)
	responseData, _ := json.Marshal(response)
	message[len(message)-1] = responseData
	responseChannel <- message
}

func RunWorker(address string, numWorkers int, quit chan int, wait chan int) {
	defer func() { close(wait) }()
	context, _ := zmq.NewContext()
	defer context.Close()
	socket, _ := context.NewSocket(zmq.ROUTER)
	defer socket.Close()
	socket.SetSockOptInt(zmq.RCVTIMEO, passiveTimeout)
	socket.Bind(address)
	runningWorkers := 0
	responseChannel := make(chan [][]byte)
	sendResponse := func(response [][]byte) {
			runningWorkers -= 1
			socket.SendMultipart(response, 0)
			if runningWorkers == 0 {
				socket.SetSockOptInt(zmq.RCVTIMEO, passiveTimeout)
			}
	}
	for {
		if runningWorkers == numWorkers {
			select {
				case response := <-responseChannel:
					sendResponse(response)
				case <- quit:
					return
			}
			continue
		}
		select {
		case <- quit:
			return
		case response := <-responseChannel:
			sendResponse(response)
			break
		default:
			message, err := socket.RecvMultipart(0)
			if err != nil {
				break
			}
			data := map[string]interface{}{}
			json.Unmarshal(message[len(message)-1], &data)
			if data == nil {
				writeLog("Received invalid message")
				break
			}
			workerFunction := registeredWorkerFunctions[data["method"].(string)]
			if workerFunction == nil {
				writeLog("Unregistered worker function:", data["method"].(string))
				break
			}
			if runningWorkers == 0 {
				socket.SetSockOptInt(zmq.RCVTIMEO, activeTimeout)
			}
			runningWorkers += 1
			go callworker(responseChannel, message, data["parameters"], workerFunction)
		}
	}
}

func RunWorkerServer(routerAddress, internalAddress string, routerBind bool, count int) {
	if count <= 0 {
		count = runtime.NumCPU()
	}
	quit := make(chan int)
	wait := make(chan int)
	RunWorker(routerAddress, count, quit, wait)
	<-wait
}
