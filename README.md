# Distributed Wagering Processor

Serviço financeiro que processa transações BET/WIN/LOSS/REFUND/ROLLBACK
vindas de HTTP e SQS, correto sob concorrência, idempotente de forma
persistente, com ledger auditável.

Veja `ARCHITECTURE.md` pras decisões de design, trade-offs e limitações
conhecidas. Veja `docs/superpowers/specs/2026-09-01-wagering-processor-architecture-design.md`
pro spec de design completo a partir do qual isso foi construído.

## Requisitos

- [Bun](https://bun.sh) 1.x
- Docker + Docker Compose

## Setup

```bash
cp .env.example .env
docker compose up -d postgres localstack
bun install
bunx mikro-orm migration:up
bun run src/messaging/infrastructure/sqs/bootstrap-queues.ts
```

Se `bunx mikro-orm ...` falhar no seu ambiente com um erro de
`node:fs`/`globSync` (problema conhecido em sistemas cujo `node` padrão é
anterior ao Node 22 com `fs.globSync`, já que o shebang da CLI do MikroORM
resolve pro `node` do sistema, não pro `bun`), roda a CLI via Bun
diretamente — equivalente, e o que de fato foi usado pra verificar este
README:

```bash
bun run node_modules/@mikro-orm/cli/cli.js migration:up
```

## Rodando

```bash
bun run start:dev        # local, contra postgres/localstack do docker-compose
# ou
bun run build && bun run start:prod   # build compilado, mais próximo de produção
# ou
docker compose up -d     # stack completa incluindo o container da app
```

`.env` aponta pra `localhost`, o que é correto pra rodar direto no host mas
errado dentro de um container; `docker-compose.yml` sobrescreve
`DATABASE_URL`, `AWS_ENDPOINT_URL` e as duas URLs de fila do serviço `app`
pros nomes dos serviços do compose (`postgres`, `localstack`), então não
precisa editar o `.env` pra rodar `docker compose up -d`. Migrations e
bootstrap das filas ainda rodam pelo host (veja Setup) — o container da app
não aplica isso sozinho.

A API escuta em `http://localhost:3000`. Verificado diretamente contra este
worktree: `bun run build` + `bun run start:prod` sobe, `/health/live` e
`/health/ready` respondem, e `POST /wallets` → `POST /wagering/transactions`
→ `GET /wallets/:id/ledger` → `POST /wallets/:id/reconciliation` fazem o
round-trip completo corretamente contra Postgres e LocalStack reais.

`bun run start:dev` chama o binário `node` do sistema (via o watcher da CLI
do Nest), então pode bater no mesmo problema de `node:fs`/`globSync`
mencionado acima dependendo do seu ambiente; `bun run start:prod` (depois de
`bun run build`) não tem esse problema, já que roda o output compilado
direto com `bun`.

## Testes

```bash
bun test src              # testes unitários (sem containers)
bun test test              # testes de integração + concorrência (sobe
                           # Postgres e LocalStack via Testcontainers —
                           # precisa do Docker rodando, sem setup manual)
bun run typecheck          # tsc --noEmit sobre src/ + test/ (specs incluídos)
bun run lint
```

Tudo verde até esta task: `bun test test/ src` → **67 passando, 0 falhando**
em 18 arquivos, incluindo todo cenário obrigatório de concorrência
(contenção de hot-wallet, 3+ instâncias concorrentes,
replay de 50 requisições duplicadas em paralelo), além de `bun run
typecheck` e `bun run lint` limpos.

`test/e2e-smoke.spec.ts` e o caso de sucesso do `/health/ready` em
`test/health.spec.ts` são as exceções — eles falam direto com a stack do
docker-compose em `localhost`, então roda `docker compose up -d`, aplica as
migrations e faz o bootstrap das filas (veja Setup) antes de rodá-los.

## Resumo da API

| Método | Path | Propósito |
|---|---|---|
| POST | `/wallets` | Abre uma wallet (saldo inicial opcional → transação `OPENING`) |
| GET | `/wallets/:walletId` | Estado da wallet |
| GET | `/wallets/:walletId/ledger` | Ledger paginado (`?after=&limit=`) |
| POST | `/wallets/:walletId/reconciliation` | Recalcula o saldo a partir do ledger, somente leitura |
| POST | `/wagering/transactions` | Submete BET/WIN/LOSS/REFUND/ROLLBACK (header `Idempotency-Key` obrigatório) |
| GET | `/wagering/transactions/:transactionId` | Busca uma transação pelo id interno |
| GET | `/providers/:providerId/wagering/transactions/:externalTransactionId` | Busca por provider + id externo |
| GET | `/health/live` / `/health/ready` | Liveness / readiness (sem autenticação) |

## Não implementado

Autenticação — deliberadamente deixada de lado pra proteger tempo pra
correção financeira, concorrência e idempotência. Veja `ARCHITECTURE.md`
§ Autenticação.

Entrega confiável das notificações de saída `WagerProcessed` pra um
consumidor externo — o publisher do outbox e o consumer de submissão de
apostas via SQS atualmente compartilham a mesma fila (os dois nomes
especificados), então uma notificação publicada volta em loop pro
consumer em vez de chegar a um subscriber de verdade. O consumer checa
`envelope.type` primeiro, reconhece a mensagem como não sendo uma submissão
de aposta, loga um único `WARN` e faz o ack imediatamente — sem transação no
banco, sem redelivery, e nunca é redirecionada pra DLQ. Inofensivo, mas
ruidoso, e fora de escopo pra esta versão. Veja `ARCHITECTURE.md`
§ Limitações conhecidas.
