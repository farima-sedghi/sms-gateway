// Package wallet owns customer money: prepaid balances, top-ups, debits and
// refunds. Every movement is written to the ledger.
//
// Owns tables: balances, ledger_entries.
//
// Invariants:
//   - Money is an integer in minor units (int64), never a float.
//   - A balance never goes negative. A debit is one conditional UPDATE
//     (amount >= cost), so concurrent debits cannot overspend.
//   - Every balance change writes a ledger row in the same transaction.
//   - A refund for a message happens at most once.
//
// Dependencies: platform only. Every other module that touches money calls
// this package's exported functions.
package wallet
