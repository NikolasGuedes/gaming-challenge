import { Injectable } from "@nestjs/common";
import { EntityManager, LockMode } from "@mikro-orm/postgresql";
import { Money } from "../../../../shared-kernel/money.js";
import { Wallet } from "../../../domain/wallet.js";
import { WalletLedgerEntry } from "../../../domain/wallet-ledger-entry.js";
import { WalletRepository } from "../../../application/ports/wallet.repository.js";
import { WalletEntity } from "../entities/wallet.entity.js";
import { WalletLedgerEntryEntity } from "../entities/wallet-ledger-entry.entity.js";
import { WalletMapper, WalletLedgerEntryMapper } from "../mappers/wallet.mapper.js";

@Injectable()
export class MikroOrmWalletRepository implements WalletRepository {
  constructor(private readonly em: EntityManager) {}

  async findById(id: string): Promise<Wallet | null> {
    const entity = await this.em.findOne(WalletEntity, { id });
    return entity ? WalletMapper.toDomain(entity) : null;
  }

  async findByIdForUpdate(id: string): Promise<Wallet | null> {
    const entity = await this.em.findOne(
      WalletEntity,
      { id },
      { lockMode: LockMode.PESSIMISTIC_WRITE },
    );
    return entity ? WalletMapper.toDomain(entity) : null;
  }

  async findByPlayerAndCurrency(playerId: string, currency: string): Promise<Wallet | null> {
    const entity = await this.em.findOne(WalletEntity, { playerId, currency });
    return entity ? WalletMapper.toDomain(entity) : null;
  }

  async save(wallet: Wallet): Promise<void> {
    const existing = await this.em.findOne(WalletEntity, { id: wallet.id });
    const entity = WalletMapper.toEntity(wallet, existing ?? undefined);
    this.em.persist(entity);
  }

  async appendLedgerEntry(entry: WalletLedgerEntry): Promise<void> {
    this.em.persist(WalletLedgerEntryMapper.toEntity(entry));
  }

  async listLedgerEntries(
    walletId: string,
    cursor: { after?: { createdAt: Date; id: string }; limit: number },
  ): Promise<WalletLedgerEntry[]> {
    const entities = await this.em.find(
      WalletLedgerEntryEntity,
      cursor.after
        ? {
            walletId,
            $or: [
              { createdAt: { $gt: cursor.after.createdAt } },
              { createdAt: cursor.after.createdAt, id: { $gt: cursor.after.id } },
            ],
          }
        : { walletId },
      { orderBy: { createdAt: "asc", id: "asc" }, limit: cursor.limit },
    );
    return entities.map(WalletLedgerEntryMapper.toDomain);
  }

  async sumLedgerEntries(walletId: string, currency: string): Promise<{ balance: Money; count: number }> {
    const rows = (await this.em.getConnection().execute(
      `select
         coalesce(sum(case when direction = 'CREDIT' then amount else -amount end), 0) as balance,
         count(*) as count
       from wallet_ledger_entries
       where wallet_id = ?`,
      [walletId],
    )) as Array<{ balance: string; count: string }>;
    const row = rows[0];
    return {
      balance: Money.rehydrate({ amount: row.balance, currency }),
      count: parseInt(row.count, 10),
    };
  }
}
