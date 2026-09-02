import { LoggerService } from "@nestjs/common";

export class JsonLogger implements LoggerService {
  log(message: unknown, context?: string): void { this.write("info", message, context); }
  warn(message: unknown, context?: string): void { this.write("warn", message, context); }
  debug(message: unknown, context?: string): void { this.write("debug", message, context); }
  verbose(message: unknown, context?: string): void { this.write("verbose", message, context); }

  error(message: unknown, trace?: string, context?: string): void {
    this.write("error", message, context, trace);
  }

  private write(level: string, message: unknown, context?: string, trace?: string): void {
    const record = {
      timestamp: new Date().toISOString(),
      level,
      ...(context ? { context } : {}),
      message: message instanceof Error ? message.message : message,
      ...(trace ? { trace } : {}),
    };
    const output = JSON.stringify(record);
    if (level === "error") process.stderr.write(`${output}\n`);
    else process.stdout.write(`${output}\n`);
  }
}
