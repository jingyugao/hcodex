import { createHash } from "node:crypto";
import { promises as dns } from "node:dns";
import { isIP } from "node:net";

/** Highest-random-weight choice: adding or removing a replica only moves the keys it wins or held. */
export function rendezvous(key: string, replicas: readonly string[]): string | undefined {
  let best: string | undefined;
  let bestScore = -1n;
  for (const replica of replicas) {
    const score = createHash("sha256").update(`${key}\0${replica}`).digest().readBigUInt64BE(0);
    if (score > bestScore || (score === bestScore && best !== undefined && replica < best)) {
      best = replica;
      bestScore = score;
    }
  }
  return best;
}

/**
 * Replica addresses of an app-server pool (a headless Service name), refreshed in the
 * background. Keeps the last known answer when DNS fails, so a resolver blip does not move
 * conversations.
 */
export function createReplicaResolver(options: {
  refreshMs?: number;
  lookup?: (host: string) => Promise<string[]>;
} = {}) {
  const refreshMs = options.refreshMs ?? 5_000;
  const lookup = options.lookup ?? ((host: string) => dns.resolve4(host));
  const cache = new Map<string, { addresses: string[]; at: number; pending?: Promise<string[]> }>();

  const refresh = (host: string) => {
    const entry = cache.get(host);
    if (entry?.pending) return entry.pending;
    const pending = lookup(host)
      .then((addresses) => {
        const sorted = [...new Set(addresses)].sort();
        cache.set(host, { addresses: sorted, at: Date.now() });
        return sorted;
      })
      .catch((error: unknown) => {
        const last = cache.get(host);
        if (last?.addresses.length) {
          cache.set(host, { addresses: last.addresses, at: Date.now() });
          return last.addresses;
        }
        cache.delete(host);
        throw error;
      });
    cache.set(host, { addresses: entry?.addresses ?? [], at: entry?.at ?? 0, pending });
    return pending;
  };

  return {
    async addresses(host: string): Promise<string[]> {
      if (isIP(host)) return [host];
      const entry = cache.get(host);
      if (entry?.addresses.length && Date.now() - entry.at < refreshMs) return entry.addresses;
      if (entry?.addresses.length) {
        void refresh(host).catch(() => {});
        return entry.addresses;
      }
      return refresh(host);
    },
  };
}

export type ReplicaResolver = ReturnType<typeof createReplicaResolver>;

/** The replica URL for a conversation: the pool URL with its host replaced by the chosen address. */
export async function replicaUrl(
  resolver: ReplicaResolver,
  poolUrl: string,
  conversationId: string,
): Promise<string> {
  const url = new URL(poolUrl);
  if (url.protocol !== "ws:" && url.protocol !== "wss:") throw new Error("Upstream must be ws:// or wss://");
  const host = url.hostname.replace(/^\[|\]$/g, "");
  const chosen = rendezvous(conversationId, await resolver.addresses(host));
  if (!chosen) throw new Error(`No replicas for ${host}`);
  url.hostname = isIP(chosen) === 6 ? `[${chosen}]` : chosen;
  return url.toString();
}
