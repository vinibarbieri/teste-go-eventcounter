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

// =============================================================================
// TESTES UNITÁRIOS - Componentes Individuais
// =============================================================================

// TestConsumerMethods testa todos os métodos do consumer
func TestConsumerMethods(t *testing.T) {
	consumer := NewConsumer()
	ctx := context.Background()

	// Testa Created
	err := consumer.Created(ctx, "user123")
	if err != nil {
		t.Errorf("Created() retornou erro: %v", err)
	}
	if consumer.eventCounts["created"]["user123"] != 1 {
		t.Errorf("Contador created deveria ser 1, mas é %d", consumer.eventCounts["created"]["user123"])
	}

	// Testa Updated
	err = consumer.Updated(ctx, "user123")
	if err != nil {
		t.Errorf("Updated() retornou erro: %v", err)
	}
	if consumer.eventCounts["updated"]["user123"] != 1 {
		t.Errorf("Contador updated deveria ser 1, mas é %d", consumer.eventCounts["updated"]["user123"])
	}

	// Testa Deleted
	err = consumer.Deleted(ctx, "user123")
	if err != nil {
		t.Errorf("Deleted() retornou erro: %v", err)
	}
	if consumer.eventCounts["deleted"]["user123"] != 1 {
		t.Errorf("Contador deleted deveria ser 1, mas é %d", consumer.eventCounts["deleted"]["user123"])
	}
}

// TestConsumerConcurrency testa concorrência no consumer
func TestConsumerConcurrency(t *testing.T) {
	consumer := NewConsumer()
	ctx := context.Background()

	var wg sync.WaitGroup
	numGoroutines := 50 // Reduzido para teste mais rápido
	numOperations := 5

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

	// Verifica contadores
	for i := 0; i < numGoroutines; i++ {
		userID := fmt.Sprintf("user%d", i)
		if consumer.eventCounts["created"][userID] != numOperations {
			t.Errorf("Contador created para %s deveria ser %d, mas é %d", userID, numOperations, consumer.eventCounts["created"][userID])
		}
	}
}

// TestShutdownMonitor testa o ShutdownMonitor
func TestShutdownMonitor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sm := NewShutdownMonitor(cancel)

	if sm == nil {
		t.Fatal("NewShutdownMonitor() retornou nil")
	}

	if sm.timer == nil {
		t.Error("Timer não foi inicializado")
	}

	// Testa reset do timer
	sm.ResetTimer()
	time.Sleep(100 * time.Millisecond)

	select {
	case <-ctx.Done():
		t.Error("Context foi cancelado prematuramente")
	default:
		// OK
	}
}

// TestShutdownMonitorAutoShutdown testa shutdown automático
func TestShutdownMonitorAutoShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	NewShutdownMonitor(cancel)

	select {
	case <-ctx.Done():
		// OK - shutdown automático funcionou
	case <-time.After(6 * time.Second):
		t.Error("Shutdown automático não funcionou em 5 segundos")
	}
}

// =============================================================================
// TESTES DE ESTRUTURAS E FUNCIONALIDADES
// =============================================================================

// TestWriteJSONResults testa a escrita dos resultados em JSON
func TestWriteJSONResults(t *testing.T) {
	eventCounts := map[string]map[string]int{
		"created": {"user1": 5, "user2": 3},
		"updated": {"user1": 2, "user3": 1},
		"deleted": {"user2": 1},
	}

	testDir := "data"
	os.RemoveAll(testDir)

	writeJSONResults(eventCounts)

	// Verifica arquivos criados
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

	// Verifica conteúdo
	createdData, err := os.ReadFile("data/events_created.json")
	if err != nil {
		t.Errorf("Erro ao ler arquivo created: %v", err)
	}

	var createdCounts map[string]int
	if err := json.Unmarshal(createdData, &createdCounts); err != nil {
		t.Errorf("Erro ao deserializar JSON: %v", err)
	}

	if createdCounts["user1"] != 5 {
		t.Errorf("Contador para user1 deveria ser 5, mas é %d", createdCounts["user1"])
	}

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
		{"invalid", "", "", false},
		{"user123.event", "", "", false},
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
				t.Errorf("UserID incorreto: %s", userID)
			}
			if eventType != test.eventType {
				t.Errorf("EventType incorreto: %s", eventType)
			}
		}
	}
}

// =============================================================================
// TESTES DE INTEGRAÇÃO - Sistema Completo
// =============================================================================

// TestIntegrationSystem testa o sistema completo integrado
func TestIntegrationSystem(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer := NewConsumer()

	// Cria canais de evento
	channels := map[string]chan ParsedMessage{
		"created": make(chan ParsedMessage, 10),
		"updated": make(chan ParsedMessage, 10),
		"deleted": make(chan ParsedMessage, 10),
	}

	var wg sync.WaitGroup

	// Inicia listeners
	for eventType, ch := range channels {
		wg.Add(1)
		go func(eType string, eventCh chan ParsedMessage) {
			defer wg.Done()

			for {
				select {
				case msg, ok := <-eventCh:
					if !ok {
						return
					}

					var err error
					switch eType {
					case "created":
						err = consumer.Created(ctx, msg.UserID)
					case "updated":
						err = consumer.Updated(ctx, msg.UserID)
					case "deleted":
						err = consumer.Deleted(ctx, msg.UserID)
					}

					if err != nil {
						t.Errorf("Erro ao processar mensagem %s: %v", eType, err)
					}

				case <-ctx.Done():
					return
				}
			}
		}(eventType, ch)
	}

	// Envia mensagens de teste
	testMessages := []ParsedMessage{
		{UserID: "user1", EventType: "created", MessageID: "msg1"},
		{UserID: "user1", EventType: "updated", MessageID: "msg2"},
		{UserID: "user2", EventType: "created", MessageID: "msg3"},
	}

	for _, msg := range testMessages {
		select {
		case channels[msg.EventType] <- msg:
		case <-ctx.Done():
			t.Fatal("Context cancelado durante teste")
		}
	}

	time.Sleep(100 * time.Millisecond)

	// Verifica contadores
	if consumer.eventCounts["created"]["user1"] != 1 {
		t.Errorf("Contador created para user1 deveria ser 1, mas é %d", consumer.eventCounts["created"]["user1"])
	}

	// Fecha canais e aguarda
	for _, ch := range channels {
		close(ch)
	}
	wg.Wait()
}

// TestMessageProcessingFlow testa o fluxo completo de processamento
func TestMessageProcessingFlow(t *testing.T) {
	routingKey := "user123.event.created"
	messageBody := `{"id": "msg789"}`

	// Parse da routing key
	parts := strings.Split(routingKey, ".")
	if len(parts) != 3 || parts[1] != "event" {
		t.Fatal("Routing key inválida")
	}

	userID := parts[0]
	eventType := parts[2]

	// Parse do JSON
	var msgData Message
	if err := json.Unmarshal([]byte(messageBody), &msgData); err != nil {
		t.Fatalf("Erro ao deserializar JSON: %v", err)
	}

	// Cria ParsedMessage
	parsedMsg := ParsedMessage{
		UserID:    userID,
		EventType: eventType,
		MessageID: msgData.ID,
	}

	// Verifica dados
	if parsedMsg.UserID != "user123" {
		t.Errorf("UserID incorreto: %s", parsedMsg.UserID)
	}

	if parsedMsg.MessageID != "msg789" {
		t.Errorf("MessageID incorreto: %s", parsedMsg.MessageID)
	}

	if parsedMsg.EventType != "created" {
		t.Errorf("EventType incorreto: %s", parsedMsg.EventType)
	}

	// Simula processamento
	consumer := NewConsumer()
	ctx := context.Background()

	err := consumer.Created(ctx, parsedMsg.UserID)
	if err != nil {
		t.Errorf("Erro ao processar mensagem: %v", err)
	}

	if consumer.eventCounts["created"]["user123"] != 1 {
		t.Errorf("Contador não foi incrementado corretamente")
	}
}

// TestConcurrentMessageProcessing testa processamento concorrente
func TestConcurrentMessageProcessing(t *testing.T) {
	consumer := NewConsumer()
	ctx := context.Background()

	channels := map[string]chan ParsedMessage{
		"created": make(chan ParsedMessage, 100),
		"updated": make(chan ParsedMessage, 100),
		"deleted": make(chan ParsedMessage, 100),
	}

	var wg sync.WaitGroup

	// Inicia workers
	for eventType, ch := range channels {
		wg.Add(1)
		go func(eType string, eventCh chan ParsedMessage) {
			defer wg.Done()

			for msg := range eventCh {
				var err error
				switch eType {
				case "created":
					err = consumer.Created(ctx, msg.UserID)
				case "updated":
					err = consumer.Updated(ctx, msg.UserID)
				case "deleted":
					err = consumer.Deleted(ctx, msg.UserID)
				}

				if err != nil {
					t.Errorf("Erro ao processar %s: %v", eType, err)
				}
			}
		}(eventType, ch)
	}

	// Envia mensagens
	numMessages := 500 // Reduzido para teste mais rápido
	for i := 0; i < numMessages; i++ {
		userID := fmt.Sprintf("user%d", i%10)
		eventType := []string{"created", "updated", "deleted"}[i%3]

		msg := ParsedMessage{
			UserID:    userID,
			EventType: eventType,
			MessageID: fmt.Sprintf("msg%d", i),
		}

		channels[eventType] <- msg
	}

	// Fecha canais
	for _, ch := range channels {
		close(ch)
	}

	wg.Wait()

	// Verifica total processado
	totalProcessed := 0
	for _, counts := range consumer.eventCounts {
		for _, count := range counts {
			totalProcessed += count
		}
	}

	if totalProcessed != numMessages {
		t.Errorf("Deveria ter processado %d mensagens, mas processou %d", numMessages, totalProcessed)
	}
}

// TestGracefulShutdown testa shutdown gracioso
func TestGracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	ch1 := make(chan string, 2)
	ch2 := make(chan string, 2)

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

	// Cancela context
	cancel()

	// Fecha canais
	close(ch1)
	close(ch2)

	// Aguarda goroutines terminarem
	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		// OK - shutdown funcionou
	case <-time.After(1 * time.Second):
		t.Error("Shutdown não funcionou em 1 segundo")
	}
}

// =============================================================================
// CONFIGURAÇÃO DE TESTES
// =============================================================================

// TestMain configura o ambiente de teste
func TestMain(m *testing.M) {
	code := m.Run()
	os.RemoveAll("data")
	os.Exit(code)
}
