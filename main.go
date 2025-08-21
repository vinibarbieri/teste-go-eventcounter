package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer interface {
	Created(ctx context.Context, uid string) error
	Updated(ctx context.Context, uid string) error
	Deleted(ctx context.Context, uid string) error
}

type Message struct {
	ID string `json:"id"`
}

type ParsedMessage struct {
	UserID    string
	EventType string
	MessageID string
}

type myConsumer struct {
	eventCounts map[string]map[string]int // eventType -> userID -> count
	mu          sync.Mutex                // Mutex para proteger acesso concorrente ao eventCounts
}

func NewConsumer() *myConsumer {
	return &myConsumer{
		eventCounts: make(map[string]map[string]int),
	}
}

// Created incrementa a count para um evento 'created' de um usuário específico.
func (c *myConsumer) Created(ctx context.Context, uid string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.eventCounts["created"]; !ok {
		c.eventCounts["created"] = make(map[string]int)
	}
	c.eventCounts["created"][uid]++
	return nil
}

// Updated incrementa a count para um evento 'updated' de um usuário específico.
func (c *myConsumer) Updated(ctx context.Context, uid string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.eventCounts["updated"]; !ok {
		c.eventCounts["updated"] = make(map[string]int)
	}
	c.eventCounts["updated"][uid]++
	return nil
}

// Deleted incrementa a count para um evento 'deleted' de um usuário específico.
func (c *myConsumer) Deleted(ctx context.Context, uid string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.eventCounts["deleted"]; !ok {
		c.eventCounts["deleted"] = make(map[string]int)
	}
	c.eventCounts["deleted"][uid]++
	return nil
}

// ShutdownMonitor gerencia um timer de inatividade. Se nenhum processamento ocorrer por 5s, ele dispara o cancelamento
type ShutdownMonitor struct {
	timer  *time.Timer
	mu     sync.Mutex
	cancel context.CancelFunc
}

// Ele configura um timer inicial que irá disparar o shutdown se nenhuma atividade ocorrer.
func NewShutdownMonitor(cancelFunc context.CancelFunc) *ShutdownMonitor {
	sm := &ShutdownMonitor{
		cancel: cancelFunc,
	}
	sm.timer = time.AfterFunc(5*time.Second, func() {
		log.Println("=== Nenhuma mensagem processada por 5 segundos. Iniciando shutdown gracioso... ===")
		sm.cancel()
	})
	return sm
}

// ResetTimer reseta o timer de inatividade
func (sm *ShutdownMonitor) ResetTimer() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.timer.Stop() {
		select {
		case <-sm.timer.C:
		default:
			// Nenhum valor estava no canal, ou ele já foi drenado.
		}
	}
	sm.timer.Reset(5 * time.Second)
}

// writeJSONResults itera através das contagens de eventos coletadas e
// as escreve em arquivos JSON separados, um para cada tipo de evento.
func writeJSONResults(eventCounts map[string]map[string]int) {
	log.Println("=== Escrevendo resultados em arquivos JSON ===")

	if _, err := os.Stat("data"); os.IsNotExist(err) {
		os.Mkdir("data", 0755)
		log.Println("Diretório data criado")
	}

	for eventType, users := range eventCounts {
		filename := fmt.Sprintf("data/events_%s.json", eventType)
		file, err := os.Create(filename)
		if err != nil {
			log.Printf("Erro ao criar arquivo %s: %v", filename, err)
			continue
		}
		defer file.Close()

		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(users); err != nil {
			log.Printf("Erro ao escrever no arquivo %s: %v", filename, err)
		} else {
			log.Printf("Escrito com sucesso em %s", filename)
		}
	}
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Printf("Aviso: Não foi possível carregar arquivo .env: %v", err)
		log.Println("Usando variáveis de ambiente do sistema ou padrões")
	}

	rabbitMQURL := os.Getenv("RABBITMQ_URL")
	if rabbitMQURL == "" {
		rabbitMQURL = ""
	}
	rabbitMQExchange := os.Getenv("RABBITMQ_EXCHANGE")
	if rabbitMQExchange == "" {
		rabbitMQExchange = ""
	}
	rabbitMQQueue := "eventcountertest"

	log.Printf("=== Conectando ao RabbitMQ em %s, Exchange: %s, Queue: %s ===", rabbitMQURL, rabbitMQExchange, rabbitMQQueue)

	// --- Configuração do Context para Cancelamento ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Configura o monitor de shutdown
	shutdownMonitor := NewShutdownMonitor(cancel)

	// --- Configuração do Consumer e Estado Compartilhado para Unicidade de Mensagens ---
	consumer := NewConsumer()

	processedMessages := make(map[string]struct{})
	var processedMu sync.Mutex

	// --- Configuração dos Canais para Tipos de Evento ---
	const channelBufferSize = 100
	createdCh := make(chan ParsedMessage, channelBufferSize)
	updatedCh := make(chan ParsedMessage, channelBufferSize)
	deletedCh := make(chan ParsedMessage, channelBufferSize)

	// --- WaitGroup para Shutdown Gracioso ---
	var wg sync.WaitGroup

	// --- Iniciar Goroutines Listener para cada Tipo de Evento ---
	eventChannels := map[string]chan ParsedMessage{
		"created": createdCh,
		"updated": updatedCh,
		"deleted": deletedCh,
	}

	for eventType, ch := range eventChannels {
		wg.Add(1)
		go func(eType string, eventCh chan ParsedMessage) {
			defer wg.Done()
			log.Printf("Iniciada goroutine listener de evento %s.", eType)
			for {
				select {
				case msg, ok := <-eventCh: // Recebe mensagem do canal
					if !ok {
						log.Printf("Canal %s fechado. Saindo do listener %s.", eType, eType)
						return // Canal fechado, goroutine deve sair
					}
					var err error
					// Chama o método apropriado no `consumer` baseado no tipo de evento
					switch eType {
					case "created":
						err = consumer.Created(ctx, msg.UserID)
					case "updated":
						err = consumer.Updated(ctx, msg.UserID)
					case "deleted":
						err = consumer.Deleted(ctx, msg.UserID)
					default:
						log.Printf("Tipo de evento desconhecido recebido: %s (ID Msg: %s)", eType, msg.MessageID)
					}

					if err != nil {
						log.Printf("Erro ao processar mensagem %s para usuário %s (ID Msg: %s): %v", eType, msg.UserID, msg.MessageID, err)
					} else {
						log.Printf("Processada mensagem %s para usuário %s (ID Msg: %s)", eType, msg.UserID, msg.MessageID)
						shutdownMonitor.ResetTimer() // Reinicia o timer de inatividade após processamento bem-sucedido
					}
				case <-ctx.Done(): // Context cancelado, sinal para sair
					log.Printf("Context cancelado. Saindo do listener %s.", eType)
					return
				}
			}
		}(eventType, ch) // Passa variáveis para a goroutine para evitar problemas de closure
	}

	// --- Iniciar Goroutine Consumidor RabbitMQ ---
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("Iniciada goroutine consumidor RabbitMQ.")

		// Estabelece conexão com RabbitMQ
		conn, err := amqp.Dial(rabbitMQURL)
		if err != nil {
			log.Fatalf("Falha ao conectar ao RabbitMQ: %v", err)
		}
		defer conn.Close() // Garante que a conexão seja fechada quando a goroutine sair

		// Abre um canal dentro da conexão
		ch, err := conn.Channel()
		if err != nil {
			log.Fatalf("Falha ao abrir um canal: %v", err)
		}
		defer ch.Close() // Garante que o canal seja fechado

		// Declara a fila como durável
		q, err := ch.QueueDeclare(
			rabbitMQQueue, // nome
			true,          // durable: true garante que a fila sobreviva a reinicializações do RabbitMQ
			false,         // autoDelete: false significa que não será deletada automaticamente
			false,         // exclusive: false significa que pode ser consumida por múltiplos consumidores
			false,         // noWait: false significa aguardar confirmação do servidor
			nil,           // arguments: argumentos adicionais para declaração da fila
		)
		if err != nil {
			log.Fatalf("Falha ao declarar uma fila: %v", err)
		}

		// Vincula a fila ao exchange.
		err = ch.QueueBind(
			q.Name,           // nome da fila
			"*.event.*",      // padrão de routing key
			rabbitMQExchange, // nome do exchange
			false,            // no-wait
			nil,              // argumentos
		)
		if err != nil {
			log.Fatalf("Falha ao vincular fila: %v", err)
		}

		// Inicia consumo de mensagens da fila
		msgs, err := ch.Consume(
			q.Name, // nome da fila
			"",     // tag do consumidor (string vazia gera uma tag única)
			false,  // autoAck: false significa que as mensagens não serão reconhecidas automaticamente
			false,  // exclusive: false permite múltiplos consumidores
			false,  // noLocal: false significa que mensagens publicadas por esta conexão podem ser consumidas por ela
			false,  // noWait: false significa aguardar confirmação do servidor
			nil,    // argumentos
		)
		if err != nil {
			log.Fatalf("Falha ao registrar um consumidor: %v", err)
		}

		// Loop infinitamente para receber mensagens ou responder ao cancelamento do context
		for {
			select {
			case d := <-msgs:
				// Pegando o routing key: <user_id>.event.<event_type>
				parts := strings.Split(d.RoutingKey, ".")
				if len(parts) != 3 || parts[1] != "event" {
					log.Printf("Pulando mensagem com formato de routing key inválido: %s", d.RoutingKey)
					d.Ack(false) // ACK mensagens com formato inválido (não reenviar)
					continue
				}
				userID := parts[0]
				eventType := parts[2]

				// Deserializa o corpo da mensagem para obter o MessageID único
				var msgData Message
				if err := json.Unmarshal(d.Body, &msgData); err != nil {
					log.Printf("Erro ao deserializar corpo da mensagem: %v, Corpo: %s", err, d.Body)
					d.Nack(false, false) // NACK sem reenviar mensagens malformadas
					continue
				}

				// Verifica unicidade da mensagem usando o mapa `processedMessages`
				processedMu.Lock()
				if _, ok := processedMessages[msgData.ID]; ok {
					processedMu.Unlock()
					log.Printf("ID da mensagem %s já processado. Pulando.", msgData.ID)
					d.Ack(false) // ACK mensagens duplicadas (não reenviar)
					continue
				}
				processedMessages[msgData.ID] = struct{}{} // Marca mensagem como processada
				processedMu.Unlock()

				// Cria uma struct ParsedMessage para despacho interno
				parsedMsg := ParsedMessage{
					UserID:    userID,
					EventType: eventType,
					MessageID: msgData.ID,
				}

				// Despacha a mensagem analisada para o canal apropriado específico do evento
				select {
				case eventChannels[eventType] <- parsedMsg:
					// Mensagem despachada com sucesso.
					d.Ack(false) // ACK após despacho bem-sucedido
				case <-ctx.Done(): // Context cancelado, sinal para sair
					log.Println("Context cancelado durante despacho da mensagem. Parando consumidor RabbitMQ.")
					d.Nack(false, true) // NACK com reenvio para processamento posterior
					return              // Context cancelado, para de consumir
				default:
					log.Printf("Canal para %s está cheio, descartando mensagem %s.", eventType, msgData.ID)
					d.Nack(false, true) // NACK com reenvio para processamento posterior
				}

			case <-ctx.Done(): // Context principal foi cancelado
				log.Println("Context cancelado. Parando consumidor RabbitMQ.")
				return // Sai da goroutine
			}
		}
	}()

	// --- Goroutine Principal aguarda cancelamento ---
	<-ctx.Done()

	// --- Inicia Sequência de Shutdown ---
	log.Println("Context principal cancelado. Iniciando shutdown gracioso...")

	// Fecha todos os canais específicos de evento. Isso sinaliza às goroutines listener
	time.Sleep(100 * time.Millisecond)
	close(createdCh)
	close(updatedCh)
	close(deletedCh)

	log.Println("Aguardando todas as goroutines terminarem...")
	wg.Wait() // Bloqueia até o contador do WaitGroup ser zero
	log.Println("Todas as goroutines terminaram.")

	// --- Escreve Resultados em Arquivos JSON ---
	writeJSONResults(consumer.eventCounts)

	log.Println("== Serviço encerrado com sucesso. ==")
}
