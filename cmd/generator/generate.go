package main

import (
	"github.com/google/uuid"
	eventcounter "github.com/reb-felipe/eventcounter/pkg"
	"math/rand"
)

var users = []string{
	"user_a",
	"user_b",
	"user_c",
	"user_d",
	"user_e",
}

var events = []eventcounter.EventType{
	eventcounter.EventCreated,
	eventcounter.EventDeleted,
	eventcounter.EventUpdated,
}

func NewMessage() *eventcounter.Message {
	idxUser := rand.Intn(len(users))
	idxEvents := rand.Intn(len(events))
	return &eventcounter.Message{
		UID:       uuid.NewString(),
		EventType: events[idxEvents],
		UserID:    users[idxUser],
	}
}
