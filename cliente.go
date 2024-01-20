package main

import (
	"fmt"
	"log"
	"strings"
	"flag"
	"os"
	"encoding/csv"
	"time"
	"sync"
	"strconv"
	"encoding/json"

	zmq "github.com/pebbe/zmq4"
	"github.com/kyokomi/emoji/v2"
)

const DIRECTORY_NAME = "files"
var i int = 0

type Message struct {
	ID             string   `json:"id"`
	NetworkID      int64    `json:"networkId"`
	Nonce          int      `json:"nonce"`
	ParentMessages []string `json:"parentMessageIds"`
	Payload        struct {
		Data  string `json:"data"`
		Index string `json:"index"`
	} `json:"payload"`
}

func receiveMessages(subscriber *zmq.Socket, writer *csv.Writer, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	fmt.Println(emoji.Sprint("\n:hourglass:Inciando a leitura das mensagens."))

	for {
		// Receive message from server
		message, err := subscriber.Recv(0)
		if err != nil {
			log.Printf("Failed to receive message: %s", err)
			continue
		}

		// Split the message at the first space to get topic and JSON payload
		parts := strings.SplitN(message, " ", 2)
		if len(parts) == 2 {
			topic := parts[0]
			jsonPayload := parts[1]
			fmt.Printf("Received message topic [%s] payload: %s\n\n\n\n", topic, jsonPayload)

			// Gravar tempo de leitura em um arquivo CSV
	
			// Decodifica o JSON no struct Message
			var payloadData Message
			if err := json.Unmarshal([]byte(jsonPayload), &payloadData); err != nil {
			    log.Printf("Failed to parse JSON payload: %s", err)
			    continue
			}
			
			// Acesse o campo "publishedAt" dentro de "payload"
			var data map[string]interface{}
			if err := json.Unmarshal([]byte(payloadData.Payload.Data), &data); err != nil {
			    log.Printf("Failed to parse 'data' field: %s", err)
			    continue
			}
			
			// Agora você pode acessar o campo "publishedAt" dentro de "data"
			startTimeUnix := data["publishedAt"].(float64)

			startTime := time.Unix(int64(startTimeUnix)/int64(time.Second), (int64(startTimeUnix)%int64(time.Second)))
			fmt.Printf("Time recebido: %s\n", startTime)

			elapsed := time.Since(startTime)
			elapsedInString := strconv.FormatFloat(elapsed.Seconds(), 'f', -1, 64)

			// Writing data to csv file
			i = i + 1
			row := []string{strconv.Itoa(i), elapsedInString}
			if err := writer.Write(row); err != nil {
				log.Fatal(err)
			}
			writer.Flush()
		} else {
			log.Printf("Received message with unexpected format: %s", message)
		}
	}
}

func main() {

	ip := flag.String("ip", "localhost", "IP address of the ZMQ server")
	port := flag.String("port", "5556", "Port of the ZMQ server")
	topicStr := flag.String("topics","LB_STATUS", "Comma-separated list of topics to subscribe to")

	flag.Parse()

	// Exemplo de uso:
	fmt.Println("IP:", *ip)
	fmt.Println("Port:", *port)
	fmt.Println("Topic:", *topicStr)

	files, err := os.ReadDir(DIRECTORY_NAME)
	if err != nil {
		log.Fatal(err)
	}

	fileName := fmt.Sprintf("tangle-hornet-reading-time_%d.csv", len(files))
	filePath := fmt.Sprintf("%s/%s", DIRECTORY_NAME, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Writing header to csv file.
	header := []string{"Índice", "Tempo de consulta (s)"}
	if err := writer.Write(header); err != nil {
		log.Fatal(err)
	}

	// Criação de uma WaitGroup para sincronizar as threads
	var wg sync.WaitGroup

	// Iniciar a thread para receber mensagens
	wg.Add(1)

	// Criar um socket ZeroMQ SUB
	context, _ := zmq.NewContext()
	subscriber, _ := context.NewSocket(zmq.SUB)
	defer context.Term()
	defer subscriber.Close()

	// Definir conexão e assinatura do tópico para o socket
	connectionString := fmt.Sprintf("tcp://%s:%s", *ip, *port)
	subscriber.Connect(connectionString)
	subscriber.SetSubscribe(*topicStr)

	go receiveMessages(subscriber, writer, &wg)

	wg.Wait()
}