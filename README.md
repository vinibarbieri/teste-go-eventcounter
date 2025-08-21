# Sistema de Contagem de Eventos

Sistema em Go para processar mensagens do RabbitMQ e contar eventos por usuário, com shutdown automático e persistência de dados.

## 🚀 Como Funciona

### Arquitetura
```
RabbitMQ → Consumer → Canais Buffer → Listeners → Contadores → JSON
```

### Fluxo Principal
1. **Consome mensagens** do RabbitMQ com routing key `*.event.*`
2. **Valida e despacha** para canais específicos (created/updated/deleted)
3. **Processa em paralelo** com 3 goroutines listener
4. **Incrementa contadores** thread-safe por usuário e tipo de evento
5. **Shutdown automático** após 5 segundos de inatividade
6. **Salva resultados** em arquivos JSON

## 📋 Pré-requisitos

- Go 1.19+
- RabbitMQ rodando
- Variáveis de ambiente configuradas
- Make

## ⚙️ Configuração

### Ambiente RabbitMQ
Este repositório **apenas consome mensagens**. Para subir o RabbitMQ e enviar mensagens de teste, você precisa executar os comandos no repositório `teste-go-eventcounter` da Fluid.

#### Comandos do Ambiente (Makefile)
**⚠️ IMPORTANTE: Execute estes comandos no repositório `teste-go-eventcounter` da Fluid, NÃO neste repositório.**

```bash
# No repositório teste-go-eventcounter da Fluid:
make env-up
make generator-publish
make env-down
```

### Variáveis de Ambiente
```bash
# .env
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
RABBITMQ_EXCHANGE=eventcountertest
```

### Estrutura de Mensagens
```json
{
  "id": "unique_message_id"
}
```

### Routing Keys
- `<id do usuario>.event.<tipo do evento>`

## 🏃‍♂️ Como Executar

### 1. Preparar Ambiente
```bash
# No repositório teste-go-eventcounter da Fluid:
make env-up
make generator-publish
```

### 2. Executar o Consumer
```bash
# Neste repositório:
go mod tidy
go run
```

## 🧪 Testes

### Executar Todos os Testes
```bash
go test -v
```

## 📊 Saída

### Arquivos Gerados
- `data/events_created.json` - Contadores de eventos created
- `data/events_updated.json` - Contadores de eventos updated  
- `data/events_deleted.json` - Contadores de eventos deleted

### Formato dos Arquivos
```json
{
  "user123": 5,
  "user456": 3,
  "user789": 1
}
```

## 🔧 Características Técnicas

### Concorrência
- **3 goroutines listener** para cada tipo de evento
- **Canais com buffer** de 100 mensagens
- **Mutex para thread safety** nos contadores

### Confiabilidade
- **ACK manual** para controle de mensagens
- **Controle de duplicatas** por MessageID
- **Graceful shutdown** com cleanup de recursos

### Monitoramento
- **Timer de inatividade** para shutdown automático
- **Logs detalhados** de operações

## 🏗️ Estrutura do Código

```
├── main.go                # Programa principal
├── main_test.go           # Testes unitários e integração
├── go.mod                 # Dependências Go
├── .env                   # Configurações
├── data/                  # Diretório de saída (criado automaticamente)
└── README.md              # Esta documentação
```

## 📈 Performance

- **Processamento paralelo** de múltiplos tipos de evento
- **Buffer inteligente** para picos de tráfego
- **Shutdown automático** para economia de recursos

## 🚀 Fluxo de Desenvolvimento

1. **Clone ambos os repositórios**:
   - Este repositório (consumer)
   - `teste-go-eventcounter` da Fluid (infraestrutura)
2. **No repositório da Fluid**: Suba o ambiente com `make env-up`
3. **No repositório da Fluid**: Gere mensagens com `make generator-publish`
4. **Neste repositório**: Execute o consumer com `go run`
5. **Monitore os logs** e arquivos de saída
6. **No repositório da Fluid**: Pare o ambiente com `make env-down` quando terminar

## 🚨 Tratamento de Erros

- **Mensagens malformadas**: NACK sem reenvio
- **Mensagens duplicadas**: ACK e ignora
- **Canais cheios**: NACK com reenvio
- **Falhas de conexão**: Log e retry automático

## 🔄 Shutdown

1. **Timer expira** após 5s de inatividade
2. **Context é cancelado** automaticamente
3. **Todas as goroutines** recebem sinal de parada
4. **Canais são fechados** de forma organizada
5. **Resultados são salvos** antes de encerrar
6. **Sistema sai limpo** sem perda de dados