package main

import "time"

type User struct {
	id        string
	emai      string
	createdAt time.Time
}

type Payments struct {
	ID             string
	UserID         string
	PaymentID      string
	OrderID        string
	IdempotencyKey string
	AmountCents    int64
	Currency       string
	Status         string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type Ledger struct {
	id       string
	purgedAt time.Time
}
