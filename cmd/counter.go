package counter

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	pkg "github.com/reb-felipe/eventcounter/pkg"
)

// EventCounter gerencia contadores de eventos por usuário de forma thread-safe
type EventCounter struct {
	mu        sync.RWMutex                     // Mutex para acesso concorrente aos contadores
	counters  map[pkg.EventType]map[string]int // EventType -> UserID -> Count
	processed map[string]bool                  // UID -> Processed (evita reprocessamento)
}

// NewEventCounter cria uma nova instância do contador
// Retorna um ponteiro para uma nova instância do EventCounter
// o &EventCounter é usado para obter o endereço da memória onde o EventCounter está alocado
// Assim, o counters e o processed manipulam a mesma struct na memória, sem criar cópias
// Isso garante desempenho e thread-safety por não copiar o mutex
func NewEventCounter() *EventCounter {
	return &EventCounter{
		counters:  make(map[pkg.EventType]map[string]int),
		processed: make(map[string]bool),
	}
}

// Increment incrementa o contador para um usuário e tipo de evento específicos
// ec é um ponteiro para a struct EventCounter
// @param eventType o tipo de evento a ser incrementado
// @param userID o ID do usuário a ser incrementado
func (ec *EventCounter) Increment(eventType pkg.EventType, userID string) {
	ec.mu.Lock()         // Bloqueia o mutex para não permitir que outras goroutines acessem os contadores enquanto o mutex está bloqueado
	defer ec.mu.Unlock() // Garante que o mutex será desbloqueado após a execução da função

	// Inicializa o map para o tipo de evento se não existir
	if ec.counters[eventType] == nil {
		ec.counters[eventType] = make(map[string]int)
	}

	// Incrementa o contador
	ec.counters[eventType][userID]++
}

// IsProcessed verifica se uma mensagem já foi processada
func (ec *EventCounter) IsProcessed(messageID string) bool {
	ec.mu.RLock()
	defer ec.mu.RUnlock()

	return ec.processed[messageID]
}

// MarkProcessed marca uma mensagem como processada
func (ec *EventCounter) MarkProcessed(messageID string) {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	ec.processed[messageID] = true
}

// GetCounters retorna uma cópia dos contadores (thread-safe)
func (ec *EventCounter) GetCounters() map[pkg.EventType]map[string]int {
	ec.mu.RLock()
	defer ec.mu.RUnlock()

	// Cria uma cópia para evitar race conditions
	result := make(map[pkg.EventType]map[string]int)
	for eventType, userCounts := range ec.counters {
		result[eventType] = make(map[string]int)
		for userID, count := range userCounts {
			result[eventType][userID] = count
		}
	}

	return result
}

// SaveToFiles salva os contadores em arquivos JSON separados por tipo de evento
func (ec *EventCounter) SaveToFiles(outputDir string) error {
	counters := ec.GetCounters()

	// Cria o diretório de saída se não existir
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("erro ao criar diretório de saída: %w", err)
	}

	// Salva cada tipo de evento em um arquivo separado
	for eventType, userCounts := range counters {
		filename := filepath.Join(outputDir, string(eventType)+".json")

		// Cria o arquivo
		file, err := os.Create(filename)
		if err != nil {
			return fmt.Errorf("erro ao criar arquivo %s: %w", filename, err)
		}
		defer file.Close()

		// Converte para JSON com formatação
		jsonData, err := json.MarshalIndent(userCounts, "", "\t")
		if err != nil {
			return fmt.Errorf("erro ao serializar JSON para %s: %w", filename, err)
		}

		// Escreve no arquivo
		if _, err := file.Write(jsonData); err != nil {
			return fmt.Errorf("erro ao escrever no arquivo %s: %w", filename, err)
		}

		fmt.Printf("Arquivo salvo: %s\n", filename)
	}

	return nil
}

// Implementação da interface Consumer

// Created processa eventos de criação
func (ec *EventCounter) Created(ctx context.Context, uid string) error {
	// Simula processamento (como no ConsumerWrapper original)
	time.Sleep(time.Millisecond * time.Duration(100+rand.Intn(200)))

	// Incrementa o contador para o usuário
	ec.Increment(pkg.EventCreated, uid)

	fmt.Printf("Evento Created processado para usuário: %s\n", uid)
	return nil
}

// Updated processa eventos de atualização
func (ec *EventCounter) Updated(ctx context.Context, uid string) error {
	// Simula processamento (como no ConsumerWrapper original)
	time.Sleep(time.Millisecond * time.Duration(100+rand.Intn(200)))

	// Incrementa o contador para o usuário
	ec.Increment(pkg.EventUpdated, uid)

	fmt.Printf("Evento Updated processado para usuário: %s\n", uid)
	return nil
}

// Deleted processa eventos de exclusão
func (ec *EventCounter) Deleted(ctx context.Context, uid string) error {
	// Simula processamento (como no ConsumerWrapper original)
	time.Sleep(time.Millisecond * time.Duration(100+rand.Intn(200)))

	// Incrementa o contador para o usuário
	ec.Increment(pkg.EventDeleted, uid)

	fmt.Printf("Evento Deleted processado para usuário: %s\n", uid)
	return nil
}
