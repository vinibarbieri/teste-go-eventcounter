# Teste técnico

## Caso eventcounter

## Escopo

Com a implementação de um sistema orientado a eventos, houve a necessidade de criar um contador de eventos por usuário na empresa X. Para resolver o problema a equipe de desenvolvimento bolou a seguinte solução:

---
Um micro serviço escrito em Go roda periodicamente buscando todas as mensagens disponíveis na fila Y. Para cada mensagem na fila é extraído o ID do usuário, o ID da mensagem e o tipo do evento. Se a mensagem não tiver sido processada ainda, a mesma deve ser enviada para um canal baseado no tipo e processada em concorrência. O consumidor do canal deve adicionar 1 ao contador do usuário baseado no tipo de evento por cada mensagem única recebida. Após 5 segundos do processamento da última mensagem o serviço deve desligar automaticamente ao finalizar todos os processamentos e escrever a contagem em arquivos json separados por tipo, identificando o usuário e quantas mensagens o mesmo recebeu.

---

Sua camada de serviço deve seguir o padrão da interface `Consumer` declarada no diretório `pkg`.
```go
type Consumer interface {
	Created(ctx context.Context, uid string) error
	Updated(ctx context.Context, uid string) error
	Deleted(ctx context.Context, uid string) error
}
```

### Mensagem

A messagem enviada no RabbitMQ tem o seguinte conteudo:
```json
{
    "id": "id unico da mensagem"
}
```

O exchange e a url são dinâmicos e devem ser passados por parâmetro ou variavel de ambiente.<br/>
A routing key é composta por: `<id do usuario>.event.<tipo do evento>`<br/>
A fila se chama `eventcountertest`<br/>

## Critérios de avaliação:

- Compreensão do problema
- Uso de canais
- Uso de goroutines
- Uso do pacote sync
- Uso do contexto

### Extras

- Testes

## Subindo ambiente

Para subir o ambiente (RabbitMQ) utilize o comando:
```shell
make env-up
```
Para subir gerar o exchange, a fila e publicar 100 mensagens de teste utilize o comando:
```shell
make generator-publish
```
Para dropar o ambiente utilize o comando:
```shell
make env-down
```
Para alterar a porta e o exchange utilizado pelo container no rabbit altere as primeiras linhas do makefile.