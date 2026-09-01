import { Module } from '@nestjs/common';
import { MikroOrmModule } from '@mikro-orm/nestjs';
import { ConfigModule } from '@nestjs/config';
import mikroOrmConfig from '../mikro-orm.config.js';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), MikroOrmModule.forRoot(mikroOrmConfig)],
})
export class AppModule {}
