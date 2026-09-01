# Distributed Wagering Processor — Architecture Design

Status: approved (pré-implementação)
Data: 2026-09-01

## 0. Princípio orientador

Este projeto **não busca maximizar os 100 pontos da avaliação**. Prioridade:
(1) blindar os requisitos eliminatórios — Money nunca `number`/`float`, sem
saldo negativo por race, sem débito/crédito duplicado, idempotência
persistente, correto com 3+ instâncias, ledger auditável, testes de
integração com Postgres/SQS reais; (2) manter todo o resto (observabilidade,
testes, mensageria) simples e objetivo, evitando mecanismos sofisticados que
seriam difíceis de explicar/defender numa apresentação; (3) autoria
implementa apenas o que consegue justificar tecnicamente ao vivo. Teste de
carga (diferencial opcional) fica fora de escopo.

## 1. Contexto e stack

Desafio Jungle Gaming: serviço financeiro distribuído que processa apostas de
múltiplos provedores, correto sob concorrência entre múltiplas instâncias,
idempotente de forma persistente, e consistente entre saldo materializado e
ledger.

Stack decidida:

- Runtime/package manager: **Bun 1.x**
- Framework: **NestJS**
- ORM: **MikroORM** (Postgres) — Prisma e Drizzle são explicitamente excluídos
  pelo enunciado; TypeORM era a alternativa aceitável, MikroORM foi escolhido
  pelo Unit of Work e `EntityManager.transactional()` explícitos, que casam
  bem com agregados DDD.
- Banco: **PostgreSQL**, coluna monetária `NUMERIC(19,4)` (folga de 2 casas
  extra sobre as 2 exigidas na borda, sem perda em cálculos intermediários).
- Mensageria: **AWS SQS via LocalStack**, filas `wager-transactions.fifo` e
  `wager-transactions-dlq.fifo` (nomes exigidos pelo enunciado).
- Testes: **bun test** nativo (não vitest) para unit; Testcontainers
  (Postgres + LocalStack reais) para integração.

## 2. Domain model

Localização: `src/wagering/domain/`.

### Money (value object)

- Campos: `amount: Decimal` (via `decimal.js`), `currency: string` (ISO-4217).
- Construção só por factories estáticas com construtor privado: `Money.from(string)`,
  `Money.zero(currency)`, `Money.rehydrate(dbRow)`.
- Rejeita na borda: `NaN`, `Infinity`, notação científica, string vazia, mais
  de 2 casas decimais, valores negativos em contratos de entrada.
- Imutável — toda operação retorna nova instância.
- API: `add`, `subtract`, `negate` (validam moeda igual, lançam
  `CurrencyMismatchError` se diferente), `isZero`, `isPositive`, `isNegative`,
  `isLessThan`, `equals`, `toJSON`/`toString` (string decimal estável).
- **Nunca usa `number`/`float`/`double` para valor monetário** — falha
  eliminatória do desafio se isso ocorrer em qualquer ponto do fluxo.

### Wallet (aggregate root)

- Campos: `id`, `playerId`, `currency`, `balance: Money`, `version: int`
  (incrementado a cada mudança de saldo — auditoria/otimista extra, não é o
  mecanismo primário de exclusão mútua).
- Invariantes: `balance >= 0` sempre; toda mudança de saldo produz exatamente
  1 `WalletLedgerEntry`; moeda da transação deve bater com a moeda da wallet
  (`CurrencyMismatchError` caso contrário, erro de domínio explícito).
- `Wallet.applyTransaction(tx)` é o único ponto que muta saldo. Para
  saldo insuficiente, retorna resultado `Rejected` (não lança exceção — é
  fluxo de negócio esperado, não uma falha técnica).
- Criação (`POST /wallets`): saldo inicial, **quando maior que zero**, gera
  transação interna `OPENING` na mesma transação SQL da criação da wallet,
  com lançamento `CREDIT` correspondente no ledger. Saldo inicial zero ou
  negativo não gera `OPENING`. `OPENING` é interna — não pode ser submetida
  via API ou fila. Resposta da criação inclui `balance` e `version: 1`.

### WagerTransaction

- Campos: `externalTransactionId`, `providerId`, `idempotencyKey`,
  `payloadHash`, `kind` (`BET | WIN | LOSS | REFUND | ROLLBACK`),
  `referenceExternalTransactionId?`, `status`.
- Status: `Pending | PendingReference | Processed | Rejected | Failed`.
- Regras por `kind`:
  - `BET`, `LOSS` debitam a wallet.
  - `WIN` credita a wallet.
  - `REFUND` **só pode referenciar uma transação `BET`**.
  - `ROLLBACK` **pode referenciar `BET`, `WIN` ou `REFUND`**.
  - Referência a `kind` inválido é erro de domínio (rejeitado, não
    `PendingReference`).
  - Reversão (`REFUND`/`ROLLBACK`) cuja transação referenciada ainda não
    existe (ou não está `Processed`) fica `PendingReference` — **não é
    rejeitada**. Resolução simples e síncrona (sem worker/cron separado):
    quando a transação referenciada é gravada como `Processed`, a mesma
    transação SQL verifica se existe alguma `WagerTransaction` em
    `PendingReference` apontando pra ela e a processa ali mesmo. Se nunca
    chegar a referência, a reversão fica `PendingReference` indefinidamente
    — aceitável pro escopo deste projeto (documentar a limitação em
    `ARCHITECTURE.md` em vez de construir um mecanismo de expiração).
  - Reversões geram lançamento inverso no ledger; nunca editam o lançamento
    original.

### WalletLedgerEntry

- Imutável. Campos: `direction` (`Debit | Credit`), `amount`, `balanceBefore`,
  `balanceAfter`, `transactionId`.
- Construído só por factory que valida `balanceAfter = balanceBefore ± amount`.
- Ledger é o registro auditável — `wallet.balance` deve sempre ser
  reconstruível a partir da soma dos lançamentos (invariante final de todo o
  desafio).

## 3. Concorrência

Estratégia escolhida: **lock pessimista via `SELECT ... FOR UPDATE`**.

Fluxo por transação de negócio:

```
BEGIN
SELECT * FROM wallets WHERE id = $1 FOR UPDATE
-- aplica regra de domínio (Wallet.applyTransaction)
INSERT INTO wallet_ledger_entries (...)
UPDATE wallets SET balance = ..., version = version + 1 WHERE id = $1
INSERT INTO outbox (...)
COMMIT
```

Isolamento: `READ COMMITTED` (padrão Postgres) — o lock de linha explícito já
garante serialização por wallet mesmo com N instâncias concorrentes.

Justificativa da escolha (vs. otimista com retry, vs. UPDATE atômico
condicional): o cenário mandatório da seção 8 (duas apostas concorrentes de
80 BRL contra saldo de 100 BRL, esperando exatamente 1 `PROCESSED` + 1
`REJECTED`) é resolvido de forma direta e sem storm de retries sob alta
contenção na mesma wallet. Trade-off aceito: escritas na mesma wallet ficam
serializadas (hot wallet vira gargalo), mas o enunciado aceita isso como
esperado — não há requisito de lock ordering entre wallets distintas porque
cada transação de negócio toca exatamente uma wallet.

## 4. Idempotência persistente

Duas camadas, nunca em memória:

- **HTTP** (`Idempotency-Key` header): tabela `idempotency_keys(key UNIQUE,
  payloadHash, response, createdAt)`. Mesma key + mesmo payload → replay da
  resposta cacheada (`idempotentReplay: true`). Mesma key + payload diferente
  → conflito (erro distinto de outras rejeições). `payloadHash` é o hash de
  um JSON canônico (chaves ordenadas) do subconjunto de campos de negócio,
  excluindo headers de transporte.
- **Mensageria** (SQS): tabela `inbox(consumerName, messageId, processedAt,
  UNIQUE(consumerName, messageId))`. Checada antes de processar, dentro da
  mesma transação SQL do débito/crédito.

Retenção/cleanup de `idempotency_keys` e `inbox` não é prescrita pelo
enunciado — política (TTL, GC, preservação de trilha de auditoria) é decisão
da implementação, a documentar em `ARCHITECTURE.md`.

## 5. Outbox + publisher

Tabela `outbox(id, aggregateId, eventType, payload jsonb, occurredAt,
attempts, nextAttemptAt, publishedAt)`. Escrita **na mesma transação SQL** do
ledger/saldo — nunca publica evento antes do commit financeiro.

Publisher: job `@Interval` rodando em **toda instância**. Usa
`SELECT ... FOR UPDATE SKIP LOCKED` para pegar lote de linhas `publishedAt IS
NULL`, sem duplicar trabalho entre publishers concorrentes. Publica no SQS
FIFO (`wager-transactions.fifo`), `MessageGroupId = walletId` (inferência
documentada — o enunciado não prescreve o valor, mas `walletId` é a unidade
de concorrência da seção 8, então agrupar por wallet preserva ordem onde
importa; a garantia final de correção vem do inbox, não da ordem do broker).
Marca `publishedAt` só após confirmação de entrega. Falha → incrementa
`attempts` e adia `nextAttemptAt` por um intervalo fixo (backoff simples, sem
curva exponencial sofisticada — fácil de explicar e suficiente pro escopo).

## 6. Consumer (SQS → inbox)

Ack manual. Processa dentro de uma transação (inbox + wallet + ledger +
outbox); só remove a mensagem do SQS após commit bem-sucedido. Erro
transiente → não faz ack, mensagem volta a ficar visível para retry (via
visibility timeout da própria fila). Erro de negócio (payload malformado,
p.ex.) → ack imediato, não reprocessa indefinidamente. Ao exceder
`maxReceiveCount` (valor a definir e documentar), SQS move automaticamente
para `wager-transactions-dlq.fifo` via redrive policy. Graceful shutdown:
`SIGTERM` aguarda mensagens em voo antes de encerrar (ou devolve visibilidade
explicitamente).

## 7. Reconciliação

`POST /wallets/:walletId/reconciliation` — **somente leitura, nunca corrige
saldo silenciosamente**. Recalcula o saldo a partir do ledger e retorna:

```json
{
  "walletId": "...",
  "storedBalance": { "amount": "...", "currency": "..." },
  "calculatedBalance": { "amount": "...", "currency": "..." },
  "difference": { "amount": "...", "currency": "..." },
  "consistent": true,
  "checkedEntries": 42
}
```

Divergência (`consistent: false`) é logada e contabilizada em métrica —
nunca ajustada automaticamente.

## 8. Auth

**Não implementado.** `ARCHITECTURE.md` documenta o desenho considerado
(Keycloak, OIDC, roles provider/player/admin) e o ponto de extensão
explícito no código: `AuthGuard` no-op + `IdentityProviderPort`. Endpoints de
health (`/health/live`, `/health/ready`) ficam abertos por definição do
enunciado. Decisão deliberada para não competir por tempo com correção
financeira, concorrência e idempotência — que valem pontos na avaliação.

## 9. Módulos NestJS

4 módulos, sem camadas extras que não agregam pra explicar/defender. Cada um
com `domain/`, `application/`, `infrastructure/`:

- `WalletModule` — wallet, ledger, reconciliação.
- `WageringModule` — `WagerTransaction`, regras BET/WIN/LOSS/REFUND/ROLLBACK,
  resolução síncrona de `PendingReference`.
- `MessagingModule` — inbox (consumer SQS) e outbox (publisher) juntos, são
  as duas faces do mesmo mecanismo de entrega confiável.
- `HealthModule` — liveness/readiness.

## 10. Observabilidade

Só o essencial: logs estruturados (JSON) com `correlationId`, `messageId`,
`transactionId`, `walletId`, `providerId` — sem dados sensíveis. Sem stack
de métricas (Prometheus/Grafana). Contagem de transações por status,
profundidade da DLQ e lag de publicação do outbox ficam disponíveis via
queries SQL simples (documentadas em `ARCHITECTURE.md`), não painel.

## 11. Testes (estratégia, não escopo desta spec)

Cobre só os cenários que o desafio exige — não persegue cobertura extra.

- Unit (bun test): `Money`, regras de `kind`, conflito de moeda, idempotency
  key com payload divergente.
- Integração (Testcontainers Postgres + LocalStack reais): migrations,
  atomicidade wallet/ledger/inbox/outbox, redelivery, retry/DLQ, recuperação
  após reinício.
- Concorrência (paralelismo real): cenário da seção 8, mesma aposta 50x em
  paralelo, ≥3 instâncias simultâneas, worker morto após commit e antes do
  ack, dois publishers sobre a mesma outbox, reversão antes da referência.
- Invariante final verificada em todo teste: `wallet.balance == saldo
  reconstruído pelo ledger`.
- Sem teste de carga (diferencial opcional, fora de escopo).

## Fora de escopo desta spec

Implementação de código, migrations concretas, plano de tarefas passo a
passo (vai para o plano de implementação via `writing-plans`), teste de
carga (diferencial opcional, section 14).
