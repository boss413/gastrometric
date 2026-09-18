/**
 * Shared HTTP boundary. All API modules go through `fetchJson`; UI code
 * must never call `fetch()` directly (see FE-01 §1.4).
 */

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;

/**
 * Error shape used everywhere an API failure reaches UI code. Never a raw
 * status code, stack trace, or unparsed body.
 */
export class ApiError extends Error {
  code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = "ApiError";
    this.code = code;
  }
}

interface ErrorEnvelope {
  error?: {
    code?: string;
    message?: string;
  };
}

/**
 * Performs a fetch against the backend, resolving to parsed JSON.
 *
 * - Non-2xx responses are parsed as `{ error: { code, message } }` and
 *   thrown as `ApiError`. If the body can't be parsed that way, a generic
 *   `ApiError` is thrown instead.
 * - Network failures (backend unreachable, LAN issues, etc.) are caught
 *   and normalized into an `ApiError` as well.
 * - `204 No Content` responses resolve to `undefined`.
 */
export async function fetchJson<T>(
  path: string,
  init?: RequestInit,
): Promise<T> {
  let response: Response;

  try {
    response = await fetch(`${API_BASE_URL}${path}`, {
      headers: {
        "Content-Type": "application/json",
        ...init?.headers,
      },
      ...init,
    });
  } catch {
    throw new ApiError("network_error", "Could not reach the server.");
  }

  if (response.status === 204) {
    return undefined as T;
  }

  if (!response.ok) {
    let envelope: ErrorEnvelope | undefined;
    try {
      envelope = (await response.json()) as ErrorEnvelope;
    } catch {
      envelope = undefined;
    }

    const code = envelope?.error?.code ?? "unknown_error";
    const message =
      envelope?.error?.message ?? "Something went wrong. Please try again.";
    throw new ApiError(code, message);
  }

  return (await response.json()) as T;
}
