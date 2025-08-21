package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestConsumerCreated testa o método Created do consumer
func TestConsumerCreated(t *testing.T) {
	consumer := NewConsumer()
	ctx := context.Background()

	// Testa incremento de contador
	err := consumer.Created(ctx, "user123")
	if err != nil {
		t.Errorf("Created() retornou erro: %v", err)
	}

	if consumer.eventCounts["created"]["user123"] != 1 {
		t.Errorf("Contador para user123 deveria ser 1, mas é %d", consumer.eventCounts["created"]["user123"])
	}

	// Testa múltiplos incrementos
	err = consumer.Created(ctx, "user123")
	if err != nil {
		t.Errorf("Created() retornou erro: %v", err)
	}

	if consumer.eventCounts["created"]["user123"] != 2 {
		t.Errorf("Contador para user123 deveria ser 2, mas é %d", consumer.eventCounts["created"]["user123"])
	}

	// Testa usuário diferente
	err = consumer.Created(ctx, "user456")
	if err != nil {
		t.Errorf("Created() retornou erro: %v", err)
	}

	if consumer.eventCounts["created"]["user456"] != 1 {
		t.Errorf("Contador para user456 deveria ser 1, mas é %d", consumer.eventCounts["created"]["user456"])
	}
}

// TestConsumerUpdated testa o método Updated do consumer
func TestConsumerUpdated(t *testing.T) {
	consumer := NewConsumer()
	ctx := context.Background()

	err := consumer.Updated(ctx, "user123")
	if err != nil {
		t.Errorf("Updated() retornou erro: %v", err)
	}

	if consumer.eventCounts["updated"]["user123"] != 1 {
		t.Errorf("Contador para user123 deveria ser 1, mas é %d", consumer.eventCounts["updated"]["user123"])
	}
}

// TestConsumerDeleted testa o método Deleted do consumer
func TestConsumerDeleted(t *testing.T) {
	consumer := NewConsumer()
	ctx := context.Background()

	err := consumer.Deleted(ctx, "user123")
	if err != nil {
		t.Errorf("Deleted() retornou erro: %v", err)
	}

	if consumer.eventCounts["deleted"]["user123"] != 1 {
		t.Errorf("Contador para user123 deveria ser 1, mas é %d", consumer.eventCounts["deleted"]["user123"])
	}
}

// TestConsumerConcurrency testa concorrência no consumer
func TestConsumerConcurrency(t *testing.T) {
	consumer := NewConsumer()
	ctx := context.Background()

	var wg sync.WaitGroup
	numGoroutines := 100
	numOperations := 10

	// Inicia múltiplas goroutines incrementando contadores
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(userID string) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				consumer.Created(ctx, userID)
				consumer.Updated(ctx, userID)
				consumer.Deleted(ctx, userID)
			}
		}(fmt.Sprintf("user%d", i))
	}

	wg.Wait()

	// Verifica se todos os contadores estão corretos
	for i := 0; i < numGoroutines; i++ {
		userID := fmt.Sprintf("user%d", i)
		if consumer.eventCounts["created"][userID] != numOperations {
			t.Errorf("Contador created para %s deveria ser %d, mas é %d", userID, numOperations, consumer.eventCounts["created"][userID])
		}
		if consumer.eventCounts["updated"][userID] != numOperations {
			t.Errorf("Contador updated para %s deveria ser %d, mas é %d", userID, numOperations, consumer.eventCounts["updated"][userID])
		}
		if consumer.eventCounts["deleted"][userID] != numOperations {
			t.Errorf("Contador deleted para %s deveria ser %d, mas é %d", userID, numOperations, consumer.eventCounts["deleted"][userID])
		}
	}
}

// TestShutdownMonitorResetTimer testa o reset do timer
func TestShutdownMonitorResetTimer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	NewShutdownMonitor(cancel)

	// Verifica se o timer foi resetado
	// Como não podemos acessar diretamente o timer, testamos indiretamente
	// O timer deve estar ativo após o reset
	time.Sleep(100 * time.Millisecond)

	// O timer não deve ter disparado ainda
	select {
	case <-ctx.Done():
		t.Error("Context foi cancelado prematuramente")
	default:
		// OK - context não foi cancelado
	}
}

// TestShutdownMonitorShutdown testa o shutdown automático
func TestShutdownMonitorShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	NewShutdownMonitor(cancel)

	// Aguarda o timer disparar (5 segundos)
	select {
	case <-ctx.Done():
		// OK - shutdown automático funcionou
	case <-time.After(6 * time.Second):
		t.Error("Shutdown automático não funcionou em 5 segundos")
	}
}

// TestWriteJSONResults testa a escrita dos resultados em JSON
func TestWriteJSONResults(t *testing.T) {
	// Cria dados de teste
	eventCounts := map[string]map[string]int{
		"created": {
			"user1": 5,
			"user2": 3,
		},
		"updated": {
			"user1": 2,
			"user3": 1,
		},
		"deleted": {
			"user2": 1,
		},
	}

	testDir := "data"

	// Chama a função
	writeJSONResults(eventCounts)

	// Verifica se os arquivos foram criados
	expectedFiles := []string{
		"data/events_created.json",
		"data/events_updated.json",
		"data/events_deleted.json",
	}

	for _, file := range expectedFiles {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			t.Errorf("Arquivo %s não foi criado", file)
		}
	}

	// Verifica conteúdo do arquivo created
	createdData, err := os.ReadFile("data/events_created.json")
	if err != nil {
		t.Errorf("Erro ao ler arquivo created: %v", err)
	}

	var createdCounts map[string]int
	if err := json.Unmarshal(createdData, &createdCounts); err != nil {
		t.Errorf("Erro ao deserializar JSON created: %v", err)
	}

	if createdCounts["user1"] != 5 {
		t.Errorf("Contador para user1 deveria ser 5, mas é %d", createdCounts["user1"])
	}

	// Limpa arquivos de teste
	os.RemoveAll(testDir)
}

// TestParseRoutingKey testa o parsing da routing key
func TestParseRoutingKey(t *testing.T) {
	tests := []struct {
		routingKey string
		userID     string
		eventType  string
		valid      bool
	}{
		{"user123.event.created", "user123", "created", true},
		{"user456.event.updated", "user456", "updated", true},
		{"user789.event.deleted", "user789", "deleted", true},
		{"invalid", "", "", false},
		{"user123.event", "", "", false},
		{"event.created", "", "", false},
		{"user123.event.created.extra", "", "", false},
	}

	for _, test := range tests {
		parts := strings.Split(test.routingKey, ".")
		valid := len(parts) == 3 && parts[1] == "event"

		if valid != test.valid {
			t.Errorf("Routing key '%s': valid=%v, esperado=%v", test.routingKey, valid, test.valid)
			continue
		}

		if valid {
			userID := parts[0]
			eventType := parts[2]

			if userID != test.userID {
				t.Errorf("Routing key '%s': userID='%s', esperado='%s'", test.routingKey, userID, test.userID)
			}

			if eventType != test.eventType {
				t.Errorf("Routing key '%s': eventType='%s', esperado='%s'", test.routingKey, eventType, test.eventType)
			}
		}
	}
}

// TestMessageUniqueness testa o controle de unicidade de mensagens
func TestMessageUniqueness(t *testing.T) {
	processedMessages := make(map[string]struct{})
	var mu sync.Mutex

	// Simula processamento de mensagens
	messageIDs := []string{"msg1", "msg2", "msg3", "msg1", "msg2"}

	for _, msgID := range messageIDs {
		mu.Lock()
		if _, ok := processedMessages[msgID]; ok {
			mu.Unlock()
			// Mensagem já processada
			continue
		}
		processedMessages[msgID] = struct{}{}
		mu.Unlock()

		// Processa mensagem...
	}

	// Verifica se apenas mensagens únicas foram processadas
	expectedCount := 3 // msg1, msg2, msg3
	if len(processedMessages) != expectedCount {
		t.Errorf("Deveria ter %d mensagens únicas, mas tem %d", expectedCount, len(processedMessages))
	}

	// Verifica se as mensagens corretas estão no mapa
	expectedMessages := map[string]bool{"msg1": true, "msg2": true, "msg3": true}
	for msgID := range processedMessages {
		if !expectedMessages[msgID] {
			t.Errorf("Mensagem inesperada no mapa: %s", msgID)
		}
	}
}

// TestChannelBuffering testa o buffering dos canais
func TestChannelBuffering(t *testing.T) {
	// Cria canal com buffer
	ch := make(chan string, 3)

	// Envia mensagens até o buffer estar cheio
	ch <- "msg1"
	ch <- "msg2"
	ch <- "msg3"

	// Verifica se o canal está cheio
	select {
	case ch <- "msg4":
		t.Error("Canal deveria estar cheio")
	default:
		// OK - canal está cheio
	}

	// Consome uma mensagem
	msg := <-ch

	// Agora deve ser possível enviar outra
	select {
	case ch <- "msg4":
		// OK - canal não está mais cheio
	default:
		t.Error("Canal deveria aceitar mais mensagens")
	}

	if msg != "msg1" {
		t.Errorf("Primeira mensagem deveria ser 'msg1', mas é '%s'", msg)
	}
}

// TestContextCancellation testa o cancelamento do context
func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Inicia goroutine que aguarda o context
	done := make(chan bool)
	go func() {
		<-ctx.Done()
		done <- true
	}()

	// Cancela o context
	cancel()

	// Verifica se a goroutine recebeu o sinal
	select {
	case <-done:
		// OK - context foi cancelado
	case <-time.After(1 * time.Second):
		t.Error("Goroutine não recebeu sinal de cancelamento")
	}
}

// TestWaitGroup testa o funcionamento do WaitGroup
func TestWaitGroup(t *testing.T) {
	var wg sync.WaitGroup
	counter := 0
	var mu sync.Mutex

	// Inicia múltiplas goroutines
	numGoroutines := 5
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()

			mu.Lock()
			counter++
			mu.Unlock()

			time.Sleep(10 * time.Millisecond)
		}()
	}

	// Aguarda todas terminarem
	wg.Wait()

	if counter != numGoroutines {
		t.Errorf("Contador deveria ser %d, mas é %d", numGoroutines, counter)
	}
}

// TestGracefulShutdown testa o shutdown gracioso
func TestGracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Cria canais
	ch1 := make(chan string, 2)
	ch2 := make(chan string, 2)

	// Inicia goroutines que consomem dos canais
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for {
			select {
			case msg := <-ch1:
				if msg == "" {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case msg := <-ch2:
				if msg == "" {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Cancela o context
	cancel()

	// Fecha os canais
	close(ch1)
	close(ch2)

	// Aguarda as goroutines terminarem
	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		// OK - shutdown gracioso funcionou
	case <-time.After(1 * time.Second):
		t.Error("Shutdown gracioso não funcionou em 1 segundo")
	}
}

// TestMain configura o ambiente de teste
func TestMain(m *testing.M) {
	// Executa os testes
	code := m.Run()

	// Limpa arquivos de teste
	os.RemoveAll("data")

	os.Exit(code)
}
