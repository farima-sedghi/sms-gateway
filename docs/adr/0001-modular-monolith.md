# ADR 0001: Modular monolith with multiple deployable processes

- **Status:** Accepted
- **Date:** 2026-11-08

## Context

The system must:

- accept ~100M SMS/day (~1,200 msg/s average, peaks several times higher),
  with very uneven load across tens of thousands of customers;
- **never** let a prepaid balance go negative, while allowing a customer to spend
  all of it;
- hand express messages to the operator within a guaranteed latency.

Different parts of the system scale on different signals. The API scales with
request rate. Workers scale with operator throughput and latency. The outbox relay
scales with commit rate.

The most important correctness rule (no negative balance, no free messages) needs
the **balance debit and the message creation to be atomic**. If they lived in
separate services with separate databases, we would need a saga with compensations.
In a saga a customer can be charged without a message being recorded, or the other
way round, until compensation finishes. It also adds a lot of failure modes for a
small team on a tight deadline.

## Decision

Build a **modular monolith with several binaries**:

- One Go module, one Postgres database, one repository.
- Four binaries that deploy and scale independently: `api`, `worker`,
  `outbox-relay` and `operator-mock` (test double).
- Code is organised **by module first** (`wallet`, `sms`, `dispatch`, `report`,
  `ratelimit`, `outbox`, `operator`). Inside each module, layers are files
  (`<module>.go` domain, `service.go`, `repository.go`, `handler.go`).
- **Each module owns its tables.** Other modules call its exported Go functions and
  never query its tables.
- Dependencies go one way: `sms -> wallet, outbox`; `dispatch -> wallet, operator`;
  `report` is read-only; `wallet` depends only on `platform`. No cycles. The Go
  compiler rejects import cycles anyway, which helps enforce this.

### Controlled exception: shared transaction

`sms` starts a `pgx.Tx` and passes it to `wallet.Debit(ctx, tx, ...)` and
`outbox.Enqueue(ctx, tx, ...)`. The debit, ledger row, message row and outbox row
then commit or roll back together. This is a deliberate coupling. It stays explicit
because the transaction is a parameter of the function signature. `wallet` still
owns its SQL; `sms` only supplies the transaction.

## Consequences

**Positive**
- Money correctness comes from one ACID transaction, with no saga or compensation.
- Each process scales on its own signal (`docker compose up --scale worker=N`, or
  separate Kubernetes Deployments).
- One build and one repo keep refactoring and code review cheap.
- Module boundaries let a module be split into its own service later if a scaling
  or ownership need appears. For example, `report` could move to a read replica or
  ClickHouse.

**Negative / risks**
- One Postgres is a shared failure and scaling point. Mitigations: partitioned
  `messages` table, a short debit transaction (one conditional UPDATE), read
  replicas for `report`, and a documented path to sharding by `customer_id`.
- The language doesn't enforce module boundaries beyond import cycles. Review and
  the "no cross-module table access" rule do. A Go `internal/` sub-tree per module
  can tighten this later.
- All binaries share a release cycle.

**Alternatives considered**
- *Microservices (wallet, sms, dispatch as separate services and databases):*
  rejected because debit + message atomicity would need a saga. Also too much
  operational overhead for the scope.
- *Single binary doing everything:* rejected because API and workers scale on
  different signals, and a slow operator must not take request-handling CPU away
  from the API.
