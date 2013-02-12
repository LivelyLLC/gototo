package gototo

import (
  "encoding/json"
  zmq "github.com/alecthomas/gozmq"
)

var registeredWorkerFunctions map[string]WorkerFunction = make(map[string]WorkerFunction)
var controlChannel chan int = make(chan int)

type WorkerFunction func(interface{}, func(interface{}))

func RegisterWorkerFunction(name string, workerFunction WorkerFunction) {
  registeredWorkerFunctions[name] = workerFunction
}

func RunWorker(address string, quit chan int) {
  context, _ := zmq.NewContext()
  defer context.Close()
  router, _ := context.NewSocket(zmq.ROUTER)
  defer router.Close()
  router.SetSockOptInt(zmq.RCVTIMEO, 100)
  router.Bind(address)
  var responseChannel = make(chan [][]byte)
  for {
    select {
    case response := <-responseChannel:
      router.SendMultipart(response, 0)
    case <-quit:
      return
    default:
      message, a := router.RecvMultipart(0)
      if a != nil {
        break
      }
      data := map[string]interface{}{}
      json.Unmarshal(message[len(message)-1], &data)
      workerFunction := registeredWorkerFunctions[data["method"].(string)]
      callback := func(c chan [][]byte) func(interface{}) {
        m := message
        cb := func(response interface{}) {
          m[len(m)-1], _ = json.Marshal(response)
          c <- m
        }
        return cb
      }(responseChannel)
      workerFunction(data["parameters"], callback)
    }
  }
}

func StartWorker(address string) {
  go RunWorker(address, controlChannel)
}

func StopWorker() {
  controlChannel <- 1
}
