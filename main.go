package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"
	"strings"
	"sync"
	"os"
	"encoding/csv"
	"strconv"

	"github.com/kyokomi/emoji/v2"
	zmq "github.com/pebbe/zmq4"
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

type MessageInfo struct {
	StartTime   time.Time
	ReceiveTime time.Time
}

type MessageWithTimestamp struct {
	Message           string
	ExactReceiveTime  time.Time
}

func receiveMessages(subscriber *zmq.Socket, messageChan chan MessageWithTimestamp, stopChan chan struct{}, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	fmt.Println(emoji.Sprint("\n:hourglass:Inciando a leitura das mensagens."))

	for {
		select {
		case <-stopChan:
			close(messageChan)
			fmt.Println(emoji.Sprint("\n:hourglass:Finalizando a leitura das mensagens."))
			return // Encerrar a goroutine quando receber sinal de parada
		default:
			// Receive message from server
			message, err := subscriber.Recv(0)
			if err != nil {
				log.Printf("Failed to receive message: %s", err)
				continue
			}
			// Adicionar timestamp exato
			exactReceiveTime := time.Now()

			// Send message to channel for saving
			messageChan <- MessageWithTimestamp{Message: message, ExactReceiveTime: exactReceiveTime}
		}
	}
}

func saveToMap(messageChan chan MessageWithTimestamp, messageMap *sync.Map, size int, done chan struct{}) {
	elementCount := 0
	mutex := sync.Mutex{} // Adicionando mutex para garantir operações seguras no elementoCount

	for {
		// Receive message from channel
		msgWithTimestamp, more := <-messageChan
		if !more {
			close(done) // Fechar o canal de conclusão quando o canal de mensagens estiver fechado
			return
		}

		// Split the message at the first space to get topic and JSON payload
		parts := strings.SplitN(msgWithTimestamp.Message, " ", 2)
		if len(parts) == 2 {
			jsonPayload := parts[1]

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
			fmt.Printf("Time Enviado: %s Time Recebido: %s\n", startTime, msgWithTimestamp.ExactReceiveTime)

			 // Verificar se a chave já existe antes de inserir
			_, loaded := messageMap.LoadOrStore(payloadData.ID, MessageInfo{StartTime: startTime, ReceiveTime: msgWithTimestamp.ExactReceiveTime})

			// Incrementar a contagem de elementos se a chave não existir previamente
			if !loaded {
				mutex.Lock()
				elementCount++
				mutex.Unlock()
			}

			// Verificar se o tamanho do mapa atingiu ou excedeu o valor especificado
			if elementCount >= size {
				close(done) // Sinalizar para encerrar
				return
			}

		} else {
			log.Printf("Received message with unexpected format: %s", msgWithTimestamp)
		}
	}
}

func saveFile(messageMap *sync.Map) {
	// Verificar se o diretório existe, se não, criá-lo
	if _, err := os.Stat(DIRECTORY_NAME); os.IsNotExist(err) {
		if err := os.MkdirAll(DIRECTORY_NAME, 0755); err != nil {
			log.Fatal(err)
		}
	}

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

	// Iterar sobre o mapa para calcular e salvar o tempo de consulta de cada mensagem
	messageMap.Range(func(key, value interface{}) bool {
		messageInfo := value.(MessageInfo)

		// Calcular o tempo de consulta para esta mensagem
		elapsed := messageInfo.ReceiveTime.Sub(messageInfo.StartTime)
		elapsedInString := strconv.FormatFloat(elapsed.Seconds(), 'f', -1, 64)

		// Escrever dados no arquivo CSV
		i++
		row := []string{strconv.Itoa(i), elapsedInString}
		if err := writer.Write(row); err != nil {
			log.Fatal(err)
		}

		return true
	})

	writer.Flush()
}


func main() {
	ip := flag.String("ip", "localhost", "IP address of the ZMQ server")
	port := flag.String("port", "5556", "Port of the ZMQ server")
	topicStr := flag.String("topics", "LB_STATUS", "Comma-separated list of topics to subscribe to")
	size := flag.Int("size", 30, "Size of the message map")

	flag.Parse()

	// Exemplo de uso:
	fmt.Println("IP:", *ip)
	fmt.Println("Port:", *port)
	fmt.Println("Topic:", *topicStr)

	// Criação de uma WaitGroup para sincronizar as threads
	var wg sync.WaitGroup

	// Criar um socket ZeroMQ SUB
	context, _ := zmq.NewContext()
	subscriber, _ := context.NewSocket(zmq.SUB)
	defer context.Term()
	defer subscriber.Close()

	// Definir conexão e assinatura do tópico para o socket
	connectionString := fmt.Sprintf("tcp://%s:%s", *ip, *port)
	subscriber.Connect(connectionString)
	subscriber.SetSubscribe(*topicStr)

	// Canal para transmitir mensagens da função receiveMessages para a goroutine saveToMap
	messageChan := make(chan MessageWithTimestamp)

	// Canal para sinalizar a parada
	stopChan := make(chan struct{})

	// Canal para sinalizar a conclusão
	done := make(chan struct{})

	// Mapa seguro para armazenar as mensagens
	var messageMap sync.Map

	// Iniciar a goroutine para salvar mensagens no mapa
	go saveToMap(messageChan, &messageMap,  *size, done)

	// Iniciar a thread para receber mensagens
	wg.Add(1)
	go receiveMessages(subscriber, messageChan, stopChan, &wg)

	// Aguardar até que a conclusão seja sinalizada
	<-done
	// Sinalize a parada da goroutine de recebimento
	close(stopChan)

	saveFile(&messageMap)

	return 
}
