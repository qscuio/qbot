export class ApiError extends Error {
  constructor(message, status) {
    super(message);
    this.status = status;
  }
}

async function request(path, options = {}) {
  const response = await fetch(path, {
    credentials: "same-origin",
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const payload = response.status === 204
    ? null
    : await response.json().catch(() => ({}));
  if (!response.ok) throw new ApiError(payload?.error || `Request failed (${response.status})`, response.status);
  return payload;
}

export const dashboardApi = {
  session: () => request("/api/dashboard/auth/session"),
  login: (username, password) => request("/api/dashboard/auth/login", {
    method: "POST",
    body: JSON.stringify({ username, password }),
  }),
  logout: () => request("/api/dashboard/auth/logout", { method: "POST", body: "{}" }),
  bootstrap: () => request("/api/dashboard/bootstrap"),
  stock: (code, period = "daily") => request(
    `/api/dashboard/stocks/${encodeURIComponent(code)}?period=${encodeURIComponent(period)}`,
  ),
  company: (code) => request(`/api/dashboard/stocks/${encodeURIComponent(code)}/company`),
  financials: (code, frequency = "annual", cursor = null) => {
    const query = new URLSearchParams({ frequency });
    if (cursor) query.set("cursor", cursor);
    return request(`/api/dashboard/stocks/${encodeURIComponent(code)}/financials?${query}`);
  },
  dividends: (code, cursor = null) => {
    const query = new URLSearchParams();
    if (cursor) query.set("cursor", cursor);
    const suffix = query.size ? `?${query}` : "";
    return request(`/api/dashboard/stocks/${encodeURIComponent(code)}/dividends${suffix}`);
  },
};
