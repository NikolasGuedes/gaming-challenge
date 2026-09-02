import { CanActivate, ExecutionContext, Injectable } from "@nestjs/common";

/**
 * Not implemented — see ARCHITECTURE.md § Authentication. Always allows
 * the request through. Not registered on any controller in this codebase;
 * exists only as the explicit extension point the spec requires. Wiring
 * this in for real would mean replacing the body with a call to an
 * `IdentityProviderPort` implementation and applying `@UseGuards(...)`.
 */
@Injectable()
export class NoOpAuthGuard implements CanActivate {
  canActivate(_context: ExecutionContext): boolean {
    return true;
  }
}
