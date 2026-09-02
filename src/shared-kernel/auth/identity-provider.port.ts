/**
 * Not implemented — see ARCHITECTURE.md § Authentication. This is the
 * extension point a real integration (Keycloak/Zitadel via OIDC) would
 * implement; nothing in this codebase constructs one today.
 */
export interface IdentityProviderPort {
  verifyToken(token: string): Promise<{ subject: string; roles: string[] } | null>;
}
