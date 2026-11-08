const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** Author of a native `userMessage.clientId` created by hcodex, if any. */
export function inputAuthor(clientId: unknown): string | undefined {
  if (typeof clientId !== "string") return;
  const [userId, nonce, extra] = clientId.split(":");
  return extra === undefined && nonce && userId && UUID.test(userId) && UUID.test(nonce)
    ? userId.toLowerCase()
    : undefined;
}

const AUTHOR_PREFIX = "<hcodex-input-author>\n";
const AUTHOR_SUFFIX = "\n</hcodex-input-author>\n\n";

/** Model-visible context; the trusted host supplies identity, hcodex owns its encoding. */
export function withInputAuthor(input: unknown[], author: { userId: string; name: string }): unknown[] {
  const text = `${AUTHOR_PREFIX}${JSON.stringify({ userId: author.userId, name: author.name })}${AUTHOR_SUFFIX}`;
  return [{ type: "text", text, text_elements: [] }, ...input];
}

/** Remove one hcodex context for display/retention, never for authorization. Browser-safe. */
export function withoutInputAuthor(input: unknown[], clientId: unknown): unknown[] {
  const author = inputAuthor(clientId);
  const first = input[0];
  if (!author || !first || typeof first !== "object" || Array.isArray(first)) return input;
  const part = first as Record<string, unknown>;
  if (part.type !== "text" || typeof part.text !== "string" || !part.text.startsWith(AUTHOR_PREFIX))
    return input;
  const end = part.text.indexOf(AUTHOR_SUFFIX, AUTHOR_PREFIX.length);
  if (end < 0) return input;
  try {
    const header: unknown = JSON.parse(part.text.slice(AUTHOR_PREFIX.length, end));
    if (!header || typeof header !== "object" || Array.isArray(header)) return input;
    const fields = header as Record<string, unknown>;
    if (Object.keys(fields).length !== 2 || typeof fields.userId !== "string" ||
      fields.userId.toLowerCase() !== author || typeof fields.name !== "string") return input;
    // Legacy history concatenates text parts; live events keep them separate.
    const remaining = part.text.slice(end + AUTHOR_SUFFIX.length);
    return remaining ? [{ ...part, text: remaining }, ...input.slice(1)] : input.slice(1);
  } catch {
    return input;
  }
}
