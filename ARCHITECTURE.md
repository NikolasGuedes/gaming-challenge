# Architecture

## Princípios de projeto

Ordem de prioridade: (1) as invariantes que não podem ser violadas de
jeito nenhum — Money nunca `number`/`float`, sem saldo negativo por race,
sem débito/crédito duplicado, idempotência persistente, correção com 3+
instâncias concorrentes, ledger auditável, testes de integração contra
Postgres/SQS reais; (2) todo o resto (profundidade de observabilidade,
amplitude de testes, sofisticação de mensageria) mantido simples e
defensável em vez de construído por construir. Teste de carga não foi
feito — não é necessário pro que este serviço se propõe a garantir.

## Stack

- **Bun** — runtime, package manager, test runner nativo (não vitest).
- **NestJS** — framework HTTP, injeção de dependência.
- **MikroORM** (`@mikro-orm/postgresql`) — escolhido em vez do TypeORM (a
  outra opção aceitável) pelo Unit of Work explícito e
  `EntityManager.transactional()`, que mapeiam bem pra "um agregado, uma
  transação". Prisma e Drizzle foram descartados por decisão de projeto.
  A versão de fato instalada é **MikroORM 7.1.14**, não 5/6 — toda entity
  neste código, portanto, é definida com `defineEntity()` +
  `Schema.setClass()` (veja `src/*/infrastructure/persistence/entities/`),
  não com a API de decorators `@Entity()`/`@Property()`/`@PrimaryKey()` de
  versões mais antigas do MikroORM. Não existe entity baseada em decorator
  em lugar nenhum do código — é o único formato de API que quem for
  contribuir precisa respeitar.
- **PostgreSQL** — `NUMERIC(19,4)` em toda coluna monetária: 2 dígitos de
  folga sobre o limite de 2 casas decimais exigido na borda HTTP, pra que
  a aritmética intermediária do ledger nunca perca precisão.
  `NUMERIC(19,2)` também atenderia a especificação; a escala extra foi uma
  escolha defensiva, não uma exigência.
- **AWS SQS via LocalStack** — `wager-transactions.fifo` para comandos de
  entrada, `wager-transactions-dlq.fifo` para falhas após as tentativas do
  consumer e `wager-events.fifo` para eventos publicados pelo outbox.
- **decimal.js** — sustenta o value object `Money`.
- **Testcontainers** — Postgres e LocalStack reais nos testes de
  integração, nunca mockados. O módulo do LocalStack de fato instalado
  (`@testcontainers/localstack@12.1.0`) configura quais serviços o
  LocalStack sobe via `.withEnvironment({ SERVICES: "sqs" })` — não existe
  um helper `.withServices([...])` nessa versão, apesar desse formato
  existir em outros bindings de linguagem do Testcontainers. Veja
  `test/bootstrap-queues.spec.ts`, `test/inbox-redelivery.spec.ts` e
  `test/outbox-publisher-worker.spec.ts` pro padrão a seguir ao adicionar
  novos testes de integração.

## Modelo de domínio

`Money`, `Wallet`, `WagerTransaction`, `WalletLedgerEntry` são classes
TypeScript puras, sem nenhum import de `@nestjs/*` ou `@mikro-orm/*`
(exigência explícita, spec §6.1). A persistência é uma entity MikroORM
separada por agregado, mais um `Mapper` que traduz nos dois sentidos
(`infrastructure/persistence/mappers/`). A indireção extra compra classes
de domínio testáveis sem banco e imunes a upgrades do ORM.

`Wallet.debit`/`credit` são imutáveis — retornam uma nova `Wallet` mais o
`WalletLedgerEntry` que justifica a mudança, nunca mutam em lugar. Saldo
insuficiente e conflito de moeda são **modelados como resultado**
(`{ status: "REJECTED", failureCode }`), não lançados como exceção — é
fluxo de negócio esperado, não excepcional.

`REFUND` só pode referenciar um `BET`; `ROLLBACK` pode referenciar `BET`,
`WIN` ou `REFUND`. Provider, player, wallet, moeda, rodada e valor precisam
coincidir com a referência. Uma reversão cuja referência ainda não chegou
(ou ainda não terminou de processar) fica armazenada como
`PENDING_REFERENCE` — não é rejeitada imediatamente. Ela pode resolver
sincronamente quando a referência chega; um worker também varre as linhas
vencidas, usa backoff exponencial e, após cinco tentativas ou cinco minutos,
rejeita com `REFERENCE_NOT_FOUND`.

## Concorrência

Lock pessimista: `SELECT ... FOR UPDATE` na linha da wallet, dentro da
mesma transação que o insert do ledger, o update da wallet e o insert do
outbox. Escolhido em vez de lock otimista com retry ou um `UPDATE` atômico
condicional porque resolve o cenário obrigatório de hot-wallet (dois BETs
concorrentes de 80.00 contra um saldo de 100.00 → exatamente um
`PROCESSED`, um `REJECTED`) sem gerar uma tempestade de retries sob
contenção. O trade-off: escritas na mesma wallet serializam. Isso é
aceito — é o comportamento esperado desse padrão, e o lock é escopado a
uma única linha, então wallets não relacionadas nunca esperam uma pela
outra.

O `ProcessWagerUseCase` também trata uma segunda race: duas requisições
concorrentes pro *mesmo* `(providerId, externalTransactionId)` podem
ambas passar pela checagem inicial de "essa transação já existe?" antes
de qualquer uma commitar — o `INSERT` da que perde falha na unique
constraint no momento do commit. Essa falha é capturada e convertida num
replay do resultado da que venceu, em vez de aparecer como erro. Provado
sob concorrência real de 50 requisições em
`test/concurrency-hot-wallet.spec.ts`.

O `ProcessWagerUseCase` também fecha uma terceira race, encontrada e
corrigida durante a implementação em vez de antecipada de antemão: nada
originalmente impedia que dois `REFUND`s *diferentes* referenciassem o
mesmo `BET` já processado e ambos creditassem a wallet — cada requisição,
isolada, parecia uma reversão nova e válida. A correção é o
`FailureCode.REFERENCE_ALREADY_REVERSED`: depois de adquirir o lock da
wallet, o use case chama
`WagerTransactionRepository.findProcessedReversalFor(providerId, externalTransactionId)`
e rejeita uma segunda reversão de uma referência que já tem uma processada
contra ela. Um índice único parcial sobre `reference_transaction_id` para
reversões processadas é a última linha de defesa no schema. O cenário de
dois `REFUND`s simultâneos está coberto por teste de integração.

Os dois caminhos de reversão — o direto acima e o
`resolvePendingReferences`, que aplica reversões que chegaram *antes* da
transação que elas revertem — passam toda regra de reversão pelo mesmo
helper compartilhado `reversalRejection()`, então não podem divergir. As
regras vivem ali: a checagem de kind da referência, escopo completo
(provider, player, wallet, moeda e rodada), igualdade do valor e a checagem de
já-revertido (no caminho pendente isso é uma flag local que vira `true`
assim que uma reversão é creditada, porque uma linha marcada como
processada mais cedo no mesmo loop ainda não está visível numa
re-consulta, então N reversões pendentes de um mesmo `BET` creditam a
wallet exatamente uma vez e o resto vira `REFERENCE_ALREADY_REVERSED`), e
o `FailureCode.WALLET_MISMATCH` — uma reversão cujo `walletId` não é o da
própria wallet da transação referenciada é rejeitada de cara. Sem essa
última, alguém poderia estornar o `BET` de um jogador na wallet de outro
jogador: dinheiro criado do nada e invisível pra reconciliação por wallet,
já que cada wallet continuaria batendo com o próprio ledger. O crédito é
sempre aplicado na wallet travada da transação referenciada, nunca num id
informado pelo chamador.

Por fim, o `ProcessWagerUseCase` rejeita qualquer kind que não pode ser
submetido externamente (`OPENING`, que só o `CreateWalletUseCase` gera)
com `FailureCode.INVALID_KIND` como seu primeiro ato. Como o use case é o
ponto de entrada único tanto pra HTTP quanto pra SQS, essa única checagem
cobre os dois canais — o validador `@IsIn` do DTO HTTP é uma segunda linha
de defesa, não a única.

**Sobre testar "3+ instâncias":** a propriedade de corretude (locks a nível
de linha, unique constraints) vive no Postgres, que não distingue "3
conexões concorrentes de 3 EntityManagers forkados num único processo Bun"
de "3 containers separados" — ambos são transações genuinamente
concorrentes sobre conexões independentes. A suite de testes exercita 3+
instâncias concorrentes do `ProcessWagerUseCase` dessa forma em vez de
orquestrar 3 réplicas separadas da app via `docker compose`. É uma
simplificação de escopo, não uma afirmação de que as duas coisas são
idênticas em todo aspecto (partições de rede entre réplicas reais não são
exercitadas).

## Idempotência

Duas camadas, ambas persistentes (nunca em memória):

- **HTTP** — tabela `idempotency_keys`, unique em `key`. Mesma key + mesmo
  hash de payload → replay da resposta cacheada. Mesma key + payload
  diferente → 409.
- **Mensageria** — tabela `inbox_messages`, chave primária
  `(consumer_name, message_id)`. Checada dentro da mesma transação do
  débito/crédito que ela guarda.

Por baixo das duas, `wager_transactions` tem sua própria
`UNIQUE (provider_id, external_transaction_id)` — a fonte de verdade real
pra "essa aposta já foi processada", independente de qual canal (HTTP ou
SQS) ela chegou, e independente de a linha do cache HTTP ou do inbox ter
sido gravada. Veja a nota de tratamento de race acima.

## Mensageria: inbox e outbox

A linha do outbox é gravada na mesma transação SQL do ledger — nunca
publicada antes do commit. Um `OutboxPublisherWorker` separado (um por
instância da app, todos rodando o mesmo código) faz polling com
`SELECT ... FOR UPDATE SKIP LOCKED`, então N instâncias de publisher nunca
disputam a mesma linha. Uma falha deixa a linha disponível para uma
tentativa posterior. O evento vai para `wager-events.fifo`, nunca para a
fila de comandos consumida pela própria aplicação.

Os eventos usam um envelope `IntegrationEvent<T>` com `eventId`,
`eventType`, `aggregateId`, `correlationId`, `causationId`, `occurredAt`,
`version` e `data`. Os tipos publicados são
`WagerTransactionProcessed`, `WagerTransactionRejected`,
`WagerTransactionPendingReference` e `WalletBalanceChanged`.

O consumer SQS e o controller HTTP chamam exatamente o mesmo
`ProcessWagerUseCase` (exigência explícita, spec §10) — nenhuma regra de
negócio duplicada por ponto de entrada.

O consumer distingue falhas de *negócio* de falhas *transientes*
(spec §6). Uma mensagem que nunca vai dar certo — um body que não é JSON,
um valor monetário malformado (`InvalidMoneyError`), conflito de moeda, ou
qualquer 4xx do Nest como `NotFoundException` pra uma wallet desconhecida
— é logada e recebe ack no primeiro recebimento: tentar de novo cinco
vezes antes da DLQ só desperdiçaria recebimentos. Todo o resto (uma
conexão de banco caída, timeout do SQS, um 5xx) fica sem ack pra que a
fila reentregue. A transação que rejeitou já fez rollback, inbox incluído,
então dar ack não deixa nenhum estado parcial pra trás.

## Reconciliação

`POST /wallets/:walletId/reconciliation` é somente leitura. Recalcula o
saldo direto em SQL a partir de cada linha do ledger
(`sum(CREDIT) - sum(DEBIT)`) e compara com o `wallets.balance`
materializado. Uma divergência é logada e retornada como
`consistent: false` — nunca corrigida silenciosamente.

## Invariantes a nível de schema

Conforme exigência explícita da especificação (§11), o seguinte é
garantido no Postgres, não só em código de aplicação:

- `wallets`: `CHECK (balance >= 0)`, `UNIQUE (player_id, currency)`.
- `wager_transactions`: `UNIQUE (idempotency_key)`,
  `UNIQUE (provider_id, external_transaction_id)` e índice único parcial
  garantindo uma única reversão processada por `reference_transaction_id`.
- `wallet_ledger_entries`: `UNIQUE (wallet_id, transaction_id)`, e um
  trigger (`prevent_ledger_mutation`) que levanta erro em qualquer
  `UPDATE`/`DELETE` — o ledger é append-only a nível de banco, não por
  convenção.
- `inbox_messages`: `PRIMARY KEY (consumer_name, message_id)`.

## Autenticação

**Não implementado** — decisão deliberada pra não competir por tempo com
correção financeira, concorrência e idempotência, que são o que este
serviço precisa garantir de verdade. Se fosse implementado: um
provedor OIDC externo (Keycloak ou Zitadel), nunca uma tabela de usuários
própria com hash de senha. O ponto de extensão está explícito no
código — `src/shared-kernel/auth/identity-provider.port.ts` e
`no-op-auth.guard.ts` — mas não conectado a nenhum controller: todo
endpoint está sem guarda hoje. Conectar de verdade significa implementar
`IdentityProviderPort` contra um provedor OIDC real e aplicar
`@UseGuards(...)`; nenhuma lógica de controller precisaria mudar além
disso, já que as regras de negócio nunca referenciam a identidade de quem
chama diretamente. `/health/live` e `/health/ready` continuariam abertos
de qualquer forma.

## Observabilidade

O bootstrap instala um logger JSON compatível com o `LoggerService` do
NestJS. Contextos fornecidos pela aplicação, incluindo `messageId`,
`transactionId`, `walletId`, `providerId` e identificadores de correlação,
são serializados como campos pesquisáveis. Não há stack de métricas
Prometheus/Grafana.
Contagem de transações, profundidade da DLQ e lag do outbox são
respondidos com uma query SQL contra `wager_transactions`/`outbox_messages`,
documentada aqui em vez de conectada a um dashboard:

```sql
-- lag do outbox
select count(*), min(occurred_at)
from outbox_messages
where published_at is null;

-- transações por status, última hora
select status, count(*)
from wager_transactions
where created_at > now() - interval '1 hour'
group by status;
```

## Pegadinhas pra quem for contribuir

- Todo teste que constrói um use case a partir de um `EntityManager`
  forkado e precisa que ele participe corretamente do
  `em.transactional()` tem que forkar com `em.fork({ useContext: true })`.
  O MikroORM 7 padroniza `useContext` como `false`, o que quebra
  silenciosamente a resolução de contexto de transação baseada em
  `AsyncLocalStorage` — o fork parece normal isolado, mas qualquer
  transação iniciada contra ele não vê as escritas do `transactional()`
  como esperado. Veja `test/wallet-opening.spec.ts` /
  `test/wallet-reads.spec.ts` pro padrão.
- Imports relativos em todo `src/` usam a extensão `.js` no specifier
  mesmo o arquivo fonte sendo `.ts` (`import ... from "./foo.js"`
  resolvendo pra `foo.ts`) — exigido pelo `moduleResolution: "nodenext"`
  deste projeto. O Bun tolera falta de extensão em runtime, por isso um
  import sem extensão pode passar despercebido por um tempo; o `tsc` (e
  portanto o `bun run build`, e portanto o build da imagem Docker, que
  roda isso) não tolera, e falha com `TS2307: Cannot find module`. Os
  quatro imports de módulo de feature em `src/app.module.ts` estavam sem
  a extensão até essa verificação pegar via um `bun run build` de
  verdade.
- O array `entities` do `mikro-orm.config.ts` lista as seis classes de
  entity diretamente (`[WalletEntity, WalletLedgerEntryEntity, ...]`),
  não uma string de glob. Antes era `entities: ['dist/**/*.entity.js']`
  (depois corrigido pro ainda-glob `'dist/src/**/*.entity.js'` no meio da
  investigação) — o discovery baseado em glob do próprio MikroORM
  re-`import()`a cada arquivo compilado que casa com o padrão pra obter
  uma referência de classe pra registrar, e sob o loader ESM do Bun esse
  re-import produzia um segundo objeto de classe distinto por entity:
  mesmo nome, identidade diferente da classe que os repositórios deste
  código importavam estaticamente em outro lugar. O resultado: todo
  `em.persist(...)` rodando contra um `bun run start:prod` de verdade (ou
  a imagem Docker, que roda o mesmo output compilado) lançava
  `ValidationError: Trying to persist not discovered entity ... not the
  prototype you are passing to the ORM` — mesmo o `bunx mikro-orm
  debug`/`bun run node_modules/@mikro-orm/cli/cli.js debug` reportando
  felizes o glob como "encontrado", e a suite de testes unitários/integração
  (que nunca roda o build compilado isoladamente) continuava verde o
  tempo todo. Passar as classes diretamente faz o MikroORM registrar o
  objeto exato que todo outro módulo importa, contornando o
  descompasso, e funciona sem mudança seja rodando esse arquivo como TS
  (dev/test, via Bun) ou como `dist/mikro-orm.config.js` compilado
  (`start:prod`/Docker) — os imports deste arquivo resolvem pro
  `.entity.ts` irmão no primeiro caso e pro `dist/src/**/*.entity.js` no
  segundo, exatamente como qualquer outro import relativo com sufixo
  `.js` neste código. Se algum dia reintroduzir um `entities` baseado em
  glob-string, reverifica rodando `bun run start:prod` de ponta a ponta
  de verdade (cria uma wallet, processa uma transação) — só `bun test`
  não pega uma regressão aqui, já que nunca executa o artefato compilado.

## Limitações conhecidas

- Cadeias transitivas de referência pendente (profundidade maior que um)
  não resolvem — ex: um `ROLLBACK` referenciando um `REFUND` que ainda
  estava `PENDING_REFERENCE` na hora. `resolvePendingReferences` só olha
  um nível de profundidade: resolve reversões que referenciam diretamente
  a transação recém-processada, não reversões-de-reversões. O worker pode
  retomá-las em ciclos posteriores, mas não há garantia de resolução de uma
  cadeia arbitrariamente profunda dentro de uma única transação.
- Nenhum teste de carga foi feito — fora de escopo pro que este serviço
  precisa garantir.
- Sem stack de métricas/dashboard — queries SQL fazem esse papel.
- Os testes de concorrência de ≥3 instâncias usam `EntityManager`
  forkados concorrentes num único processo em vez de containers
  separados (veja a seção Concorrência acima).
- O terceiro cenário de `test/concurrency-hot-wallet.spec.ts` (≥3
  instâncias) prova corretude — a wallet não relacionada processa
  corretamente junto com a contenciosa — mas não adiciona uma asserção de
  timing provando execução genuinamente não-bloqueante; uma implementação
  com lock global poderia, em teoria, ainda passar nele. Aceito: os
  cenários 1 e 3 desse arquivo já provam rigorosamente as propriedades
  que importam (exclusão mútua na wallet contenciosa,
  replay idempotente sob duplicatas concorrentes), e uma prova baseada em
  timing de execução não-bloqueante é inerentemente instável num ambiente
  de CI compartilhado.
