import { Controller, Get, Inject, ServiceUnavailableException } from "@nestjs/common";
import { EntityManager } from "@mikro-orm/postgresql";
import { ListQueuesCommand, SQSClient } from "@aws-sdk/client-sqs";
import { SQS_CLIENT } from "../messaging/infrastructure/sqs/sqs-client.provider.js";

type CheckStatus = "ok" | "error";

@Controller("health")
export class HealthController {
  constructor(
    private readonly em: EntityManager,
    @Inject(SQS_CLIENT) private readonly sqsClient: SQSClient,
  ) {}

  @Get("live")
  live(): { status: "ok" } {
    return { status: "ok" };
  }

  @Get("ready")
  async ready(): Promise<{ database: CheckStatus; sqs: CheckStatus }> {
    const checks: { database: CheckStatus; sqs: CheckStatus } = { database: "ok", sqs: "ok" };

    try {
      await this.em.getConnection().execute("select 1");
    } catch {
      checks.database = "error";
    }

    try {
      await this.sqsClient.send(new ListQueuesCommand({}));
    } catch {
      checks.sqs = "error";
    }

    if (checks.database === "error" || checks.sqs === "error") {
      throw new ServiceUnavailableException(checks);
    }
    return checks;
  }
}
