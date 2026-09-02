import { Module } from '@nestjs/common';
import { MikroOrmModule } from '@mikro-orm/nestjs';
import { ConfigModule } from '@nestjs/config';
import mikroOrmConfig from '../mikro-orm.config.js';
import { WalletModule } from './wallet/wallet.module.js';
import { WageringModule } from './wagering/wagering.module.js';
import { MessagingModule } from './messaging/messaging.module.js';
import { HealthModule } from './health/health.module.js';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    MikroOrmModule.forRoot(mikroOrmConfig),
    WalletModule,
    WageringModule,
    MessagingModule,
    HealthModule,
  ],
})
export class AppModule {}
