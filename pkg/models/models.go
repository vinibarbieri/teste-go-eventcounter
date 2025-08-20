package models

import (
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/vinibarbieri/teste-go-eventcounter/pkg/eventcounter" // Importar o pacote fornecido
)

// MessageData agora usa eventcounter.EventType
type MessageData struct {
	UserID    string
	MessageID string
	// Usamos o EventType definido no pacote fornecido para consistência.
	EventType eventcounter.EventType
}

// RawMessage representa uma mensagem bruta do RabbitMQ junto com seus dados parseados.
type RawMessage struct {
	Delivery amqp.Delivery
	Data     MessageData // UserID, MessageID, EventType
}

// OperationCounter armazena as contagens de eventos por tipo e usuário.
// O map de counts também usará eventcounter.EventType
type OperationCounter struct {
	counts map[eventcounter.EventType]map[string]int
	mu     sync.RWMutex
}

// NewOperationCounter cria e retorna uma nova instância de OperationCounter.
func NewOperationCounter() *OperationCounter {
	return &OperationCounter{
		counts: make(map[eventcounter.EventType]map[string]int),
	}
}

// Increment adiciona 1 ao contador para o tipo de evento e userID.
func (oc *OperationCounter) Increment(eventType eventcounter.EventType, userID string) {
	oc.mu.Lock()
	defer oc.mu.Unlock()

	if _, ok := oc.counts[eventType]; !ok {
		oc.counts[eventType] = make(map[string]int)
	}
	oc.counts[eventType][userID]++
}

// GetCounts retorna uma cópia segura dos contadores.
func (oc *OperationCounter) GetCounts() map[eventcounter.EventType]map[string]int {
	oc.mu.RLock()
	defer oc.mu.RUnlock()

	copyCounts := make(map[eventcounter.EventType]map[string]int)
	for eventType, userCounts := range oc.counts {
		copyCounts[eventType] = make(map[string]int)
		for userID, count := range userCounts {
			copyCounts[eventType][userID] = count
		}
	}
	return copyCounts
}

// Deduplicator rastreia IDs de mensagens processadas para evitar reprocessamento.
type Deduplicator struct {
	processedIDs map[string]bool
	mu           sync.Mutex
}

// NewDeduplicator cria uma nova instância de Deduplicator.
func NewDeduplicator() *Deduplicator {
	return &Deduplicator{
		processedIDs: make(map[string]bool),
	}
}

// IsProcessed verifica se um messageID já foi processado.
// Retorna true se já foi processado, false caso contrário.
// Se não foi processado, ele é marcado como processado.
func (d *Deduplicator) IsProcessed(messageID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.processedIDs[messageID] {
		return true
	}
	d.processedIDs[messageID] = true
	return false
}