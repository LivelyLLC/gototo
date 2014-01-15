package gototo

import (
	"encoding/json"
	zmq "github.com/JeremyOT/gozmq"
	"log"
	"runtime"
)

var registeredWorkerFunctions map[string]WorkerFunction = make(map[string]WorkerFunction)

type WorkerCommand int

const (
	QUIT = WorkerCommand(-1)
)

type WorkerStatus int

const (
	STOPPED = WorkerStatus(0)
	RUNNING = WorkerStatus(1)
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

type WorkerFunction func(interface{}, func(interface{}))

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

func RunWorker(address string, control chan WorkerCommand, status chan WorkerStatus) {
	defer func() { status <- STOPPED }()
	context, _ := zmq.NewContext()
	defer context.Close()
	socket, _ := context.NewSocket(zmq.REP)
	defer socket.Close()
	socket.SetSockOptInt(zmq.RCVTIMEO, 100)
	socket.Connect(address)
	var responseChannel = make(chan []byte)
	status <- RUNNING
	for {
		select {
		case <-control:
			return
		default:
			message, err := socket.RecvMultipart(0)
			if err != nil {
				break
			}
			data := map[string]interface{}{}
			callback := func(response interface{}) {
				responseMessage, _ := json.Marshal(response)
				responseChannel <- responseMessage
			}
			json.Unmarshal(message[len(message)-1], &data)
			if data == nil {
				writeLog("Received invalid message")
				continue
			}
			workerFunction := registeredWorkerFunctions[data["method"].(string)]
			if workerFunction == nil {
				writeLog("Unregistered worker function:", data["method"].(string))
				continue
			}
			go workerFunction(data["parameters"], callback)
			response := <-responseChannel
			message[len(message)-1] = response
			socket.SendMultipart(message, 0)
		}
	}
}

func StartWorker(address string) (WorkerStatus, chan WorkerCommand, chan WorkerStatus) {
	controlChannel := make(chan WorkerCommand)
	statusChannel := make(chan WorkerStatus)
	go RunWorker(address, controlChannel, statusChannel)
	return <-statusChannel, controlChannel, statusChannel
}

func RunWorkerServer(routerAddress, internalAddress string, routerBind bool, count int) {
	go RunRouter(routerAddress, internalAddress, routerBind, true)
	RunWorkers(internalAddress, count)
}

func RunWorkers(address string, count int) {
	if count <= 0 {
		count = runtime.NumCPU()
	}
	statusChannels := make([]chan WorkerStatus, count)
	for i := 0; i < count; i++ {
		_, _, statusChannel := StartWorker(address)
		statusChannels[i] = statusChannel
	}
	for _, c := range statusChannels {
		<-c
	}
}
