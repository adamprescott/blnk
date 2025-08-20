/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"sync"

	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/internal/cache"
	"github.com/jerry-enebeli/blnk/model"
	pgconn "github.com/jerry-enebeli/blnk/internal/pg-conn"
)

// Declare a package-level variable to hold the singleton instance.
var instance *Datasource
var once sync.Once

type Datasource struct {
	Conn  *sql.DB
	Cache cache.Cache
}

// NewDataSource initializes a new database connection.
func NewDataSource(configuration *config.Configuration) (IDataSource, error) {
	con, err := GetDBConnection(configuration)
	if err != nil {
		return nil, err
	}

	// Set the default schema for this connection.
	if _, err := con.Conn.Exec("SET search_path TO blnk"); err != nil {
		return nil, err
	}
	return con, nil
}

// GetDBConnection ensures a single database connection instance.
func GetDBConnection(configuration *config.Configuration) (*Datasource, error) {
	var err error
	once.Do(func() {
		con, errConn := ConnectDB(configuration.DataSource)
		if errConn != nil {
			err = errConn
			return
		}

		cacheInstance, errCache := cache.NewCache()
		if errCache != nil {
			log.Printf("Error creating cache: %v", errCache)
			// Continue without cache instead of failing completely.
		}

		instance = &Datasource{Conn: con, Cache: cacheInstance}
	})
	if err != nil {
		return nil, err
	}
	return instance, nil
}

// ConnectDB establishes a database connection with pooling.
func ConnectDB(dsConfig config.DataSourceConfig) (*sql.DB, error) {
	return pgconn.ConnectDB(dsConfig)
}

// AtomicTx represents a database transaction context for atomic operations
type AtomicTx struct {
	tx         *sql.Tx
	datasource *Datasource
}

// UpdateBalance updates a balance within the atomic transaction
func (a *AtomicTx) UpdateBalance(ctx context.Context, balance *model.Balance) error {
	query := `
        UPDATE blnk.balances
        SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, version = version + 1
        WHERE balance_id = $1 AND version = $12
    `

	result, err := a.tx.ExecContext(ctx, query, 
		balance.BalanceID, 
		balance.Balance.String(), 
		balance.CreditBalance.String(), 
		balance.DebitBalance.String(), 
		balance.InflightBalance.String(), 
		balance.InflightCreditBalance.String(), 
		balance.InflightDebitBalance.String(), 
		balance.Currency, 
		balance.CurrencyMultiplier, 
		balance.LedgerID, 
		balance.CreatedAt, 
		balance.Version)
	if err != nil {
		return fmt.Errorf("failed to update balance %s: %w", balance.BalanceID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected for balance %s: %w", balance.BalanceID, err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("optimistic locking failure: balance %s may have been updated by another transaction", balance.BalanceID)
	}

	// Increment the version number after a successful update
	balance.Version++
	return nil
}

// PersistTransaction persists a transaction within the atomic transaction
func (a *AtomicTx) PersistTransaction(ctx context.Context, transaction *model.Transaction) error {
	// Discard transaction if amount is 0
	if transaction.PreciseAmount != nil && transaction.PreciseAmount.Cmp(big.NewInt(0)) == 0 {
		return nil
	}

	query := `INSERT INTO blnk.transactions(transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash, effective_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`

	metaDataJSON, err := json.Marshal(transaction.MetaData)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata for transaction %s: %w", transaction.TransactionID, err)
	}

	_, err = a.tx.ExecContext(ctx, query,
		transaction.TransactionID,
		transaction.ParentTransaction,
		transaction.Source,
		transaction.Reference,
		transaction.AmountString,
		transaction.PreciseAmount.String(),
		transaction.Precision,
		transaction.Rate,
		transaction.Currency,
		transaction.Destination,
		transaction.Description,
		transaction.Status,
		transaction.CreatedAt,
		metaDataJSON,
		transaction.ScheduledFor,
		transaction.Hash,
		transaction.EffectiveDate,
	)
	if err != nil {
		return fmt.Errorf("failed to persist transaction %s: %w", transaction.TransactionID, err)
	}

	return nil
}

// Commit commits the atomic transaction
func (a *AtomicTx) Commit() error {
	return a.tx.Commit()
}

// Rollback rolls back the atomic transaction
func (a *AtomicTx) Rollback() error {
	return a.tx.Rollback()
}

// BeginAtomicTx begins a new database transaction for atomic operations
func (d *Datasource) BeginAtomicTx(ctx context.Context) (AtomicTransaction, error) {
	tx, err := d.Conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		return nil, err
	}
	return &AtomicTx{tx: tx, datasource: d}, nil
}
