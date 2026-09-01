import { Module } from "@nestjs/common";
import { MessagingModule } from "../messaging/messaging.module.js";
import { HealthController } from "./health.controller.js";

@Module({
  imports: [MessagingModule],
  controllers: [HealthController],
})
export class HealthModule {}
