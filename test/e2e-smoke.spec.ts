import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication, ValidationPipe } from '@nestjs/common';
import request from 'supertest';
import { AppModule } from '../src/app.module.js';

describe('End-to-end smoke test (requires `docker compose up -d`)', () => {
  let app: INestApplication;

  beforeAll(async () => {
    const moduleRef: TestingModule = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = moduleRef.createNestApplication();
    app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
    await app.init();
  }, 30_000);

  afterAll(async () => {
    await app.close();
  });

  it('GET /health/live and /health/ready respond', async () => {
    await request(app.getHttpServer()).get('/health/live').expect(200, { status: 'ok' });
    await request(app.getHttpServer()).get('/health/ready').expect(200, { database: 'ok', sqs: 'ok' });
  });

  it('creates a wallet, submits a BET over HTTP, and reads it back', async () => {
    const playerId = `player-e2e-${Date.now()}`;
    const createResponse = await request(app.getHttpServer())
      .post('/wallets')
      .send({ playerId, initialBalance: { amount: '100.00', currency: 'BRL' } })
      .expect(201);

    const walletId = createResponse.body.id;

    const betResponse = await request(app.getHttpServer())
      .post('/wagering/transactions')
      .set('Idempotency-Key', `idem-e2e-${walletId}`)
      .send({
        externalTransactionId: `bet-e2e-${walletId}`,
        providerId: 'provider-e2e',
        playerId,
        walletId,
        roundId: 'round-e2e',
        gameId: 'game-e2e',
        kind: 'BET',
        money: { amount: '40.00', currency: 'BRL' },
      })
      .expect(201);

    expect(betResponse.body.status).toBe('PROCESSED');
    expect(betResponse.body.balance).toEqual({ amount: '60.00', currency: 'BRL' });

    const walletResponse = await request(app.getHttpServer()).get(`/wallets/${walletId}`).expect(200);
    expect(walletResponse.body.balance).toEqual({ amount: '60.00', currency: 'BRL' });
  });
});
