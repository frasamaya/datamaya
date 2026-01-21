import { Hono } from "hono";
import { serveStatic } from "hono/bun";
import { getCookie, setCookie, deleteCookie } from "hono/cookie";
import {
  CopyObjectCommand,
  CreateMultipartUploadCommand,
  DeleteObjectCommand,
  CompleteMultipartUploadCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListPartsCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
  UploadPartCommand,
} from "@aws-sdk/client-s3";
import { createHmac, randomUUID, scryptSync, timingSafeEqual } from "crypto";
import { promises as fs } from "fs";
import os from "os";
import path from "path";

type UserRole = "read-only" | "read-write" | "admin";
type StorageMode = "local" | "s3";

type UserConfig = {
  username: string;
  password?: string;
  passwordHash?: string;
  role?: UserRole;
  root?: string;
};

type UserRecord = {
  username: string;
  role: UserRole;
  rootPath: string;
  rootReal: string;
  password?: string;
  passwordHash?: string;
};

type SessionPayload = {
  exp: number;
  nonce: string;
  user: string;
};

type SessionContext = SessionPayload & {
  role: UserRole;
  rootPath: string;
  rootReal: string;
};
type AppContext = Parameters<typeof setCookie>[0];
type ShareRecord = {
  token: string;
  path: string;
  rootReal: string;
  createdAt: number;
};

const ROOT = (process.env.FILE_ROOT ?? "").trim() || process.cwd();
const ROOT_REAL = await fs.realpath(ROOT);
const STORAGE_MODE = resolveStorageMode();
const S3_BUCKET = (process.env.S3_BUCKET ?? "").trim();
const S3_REGION = (process.env.AWS_REGION ?? process.env.S3_REGION ?? "").trim();
const S3_ENDPOINT = (process.env.S3_ENDPOINT ?? "").trim();
const S3_FORCE_PATH_STYLE =
  (process.env.S3_FORCE_PATH_STYLE ?? "").trim().toLowerCase() === "true";
const S3_ROOT_PREFIX = normalizeS3Prefix(process.env.S3_ROOT_PREFIX ?? "");
const PASSWORD = (process.env.ADMIN_PASSWORD ?? "").trim();
const USERS_FILE = process.env.USERS_FILE?.trim();
const USERS_JSON = process.env.USERS_JSON?.trim();
const SESSION_SECRET = (process.env.SESSION_SECRET ?? randomUUID()).trim();
const SESSION_COOKIE = "fm_session";
const SESSION_TTL_MS = 1000 * 60 * 60 * 8;
const SESSION_ROTATE_MS = 1000 * 60 * 30;
const MAX_PREVIEW_BYTES = 200 * 1024;
const MAX_EDIT_BYTES = 1024 * 1024;
const SEARCH_MAX_BYTES_RAW = process.env.SEARCH_MAX_BYTES?.trim();
const SEARCH_MAX_BYTES = Number.parseInt(SEARCH_MAX_BYTES_RAW ?? "", 10);
const MAX_SEARCH_BYTES = Number.isNaN(SEARCH_MAX_BYTES) ? MAX_PREVIEW_BYTES : SEARCH_MAX_BYTES;
const ARCHIVE_LARGE_MB_RAW = process.env.ARCHIVE_LARGE_MB?.trim();
const ARCHIVE_LARGE_MB = Number.parseInt(ARCHIVE_LARGE_MB_RAW ?? "", 10);
const ARCHIVE_LARGE_BYTES =
  Number.isFinite(ARCHIVE_LARGE_MB) && ARCHIVE_LARGE_MB > 0
    ? ARCHIVE_LARGE_MB * 1024 * 1024
    : 100 * 1024 * 1024;
const UPLOAD_TMP_DIR = path.join(os.tmpdir(), "bro-fm-uploads");
const AUDIT_LOG_PATH = process.env.AUDIT_LOG_PATH?.trim() ?? path.join(process.cwd(), "audit.log");
const SHARE_STORE_PATH =
  process.env.SHARE_STORE_PATH?.trim() ?? path.join(process.cwd(), "share-links.json");
const SESSION_COOKIE_BASE_OPTIONS = {
  httpOnly: true,
  sameSite: "Strict",
  maxAge: Math.floor(SESSION_TTL_MS / 1000),
  path: "/",
} as const;
const TEXT_PREVIEW_EXTS = new Set([".txt", ".php", ".js", ".html", ".csv"]);
const TEXT_EDIT_EXTS = new Set([
  ".txt",
  ".php",
  ".md",
  ".markdown",
  ".html",
  ".htm",
  ".css",
  ".scss",
  ".less",
  ".js",
  ".jsx",
  ".ts",
  ".tsx",
  ".json",
  ".yml",
  ".yaml",
  ".xml",
  ".svg",
]);
const IMAGE_PREVIEW_EXTS = new Set([
  ".png",
  ".jpg",
  ".jpeg",
  ".gif",
  ".webp",
  ".bmp",
  ".svg",
]);
const IMAGE_MIME_BY_EXT: Record<string, string> = {
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".bmp": "image/bmp",
  ".svg": "image/svg+xml",
};
const SHARE_MAP = new Map<string, ShareRecord>();
await loadShareStore();

if (STORAGE_MODE === "s3" && !S3_BUCKET) {
  throw new Error("S3_BUCKET is required when STORAGE_MODE is set to s3.");
}

const s3Client =
  STORAGE_MODE === "s3"
    ? new S3Client({
        region: S3_REGION || "us-east-1",
        endpoint: S3_ENDPOINT || undefined,
        forcePathStyle: S3_FORCE_PATH_STYLE || undefined,
      })
    : null;

const USERS = await loadUsers();
const USER_MAP = new Map(USERS.map((user) => [user.username, user]));

const app = new Hono<{ Variables: { session: SessionContext } }>();

app.use("*", async (c, next) => {
  c.header("X-Content-Type-Options", "nosniff");
  if (c.req.path.startsWith("/api/share/") && c.req.path.endsWith("/file")) {
    c.header("X-Frame-Options", "SAMEORIGIN");
  } else {
    c.header("X-Frame-Options", "DENY");
  }
  c.header("Referrer-Policy", "no-referrer");
  await next();
});

app.use("/api/*", async (c, next) => {
  if (c.req.path === "/api/login") {
    return next();
  }
  if (c.req.method === "GET" && c.req.path.startsWith("/api/share/")) {
    return next();
  }

  const token = getCookie(c, SESSION_COOKIE);
  const session = verifySession(token);
  if (!session) {
    return c.json({ error: "Unauthorized" }, 401);
  }

  const user = USER_MAP.get(session.user);
  if (!user) {
    return c.json({ error: "Unauthorized" }, 401);
  }

  c.set("session", {
    ...session,
    role: user.role,
    rootPath: user.rootPath,
    rootReal: user.rootReal,
  });

  if (session.exp - Date.now() <= SESSION_ROTATE_MS) {
    setSessionCookie(c, createSession(session.user));
  }

  await next();
});

app.post("/api/login", async (c) => {
  let body: { username?: string; password?: string } = {};
  try {
    body = await c.req.json();
  } catch {
    return c.json({ error: "Invalid JSON." }, 400);
  }

  if (USER_MAP.size === 0) {
    return c.json({ error: "No users configured." }, 500);
  }

  const username = (body.username ?? "").trim();
  const user = resolveLoginUser(username);
  if (!user) {
    await auditLog(c, "login_failed", { username, reason: "user_not_found" });
    return c.json({ error: "Invalid credentials." }, 401);
  }

  if (username && username !== user.username && USER_MAP.size === 1) {
    await auditLog(c, "login_fallback", {
      username,
      resolved: user.username,
      reason: "single_user_fallback",
    });
  }

  const provided = (body.password ?? "").trim();
  if (!verifyUserPassword(user, provided)) {
    await auditLog(c, "login_failed", { username: user.username, reason: "bad_password" });
    return c.json({ error: "Invalid credentials." }, 401);
  }

  setSessionCookie(c, createSession(user.username));
  await auditLog(c, "login_success", { username: user.username, role: user.role });

  return c.json({ ok: true, user: user.username, role: user.role });
});

app.post("/api/logout", async (c) => {
  const session = c.get("session");
  deleteCookie(c, SESSION_COOKIE, { path: "/" });
  await auditLog(c, "logout", { username: session.user });
  return c.json({ ok: true });
});

app.get("/api/list", async (c) => {
  const session = c.get("session");
  if (STORAGE_MODE === "s3") {
    return handleS3List(c, session);
  }
  const requestPath = c.req.query("path") ?? "/";
  const requestedPage = parsePositiveInt(c.req.query("page"));
  const pageSize = parsePositiveInt(c.req.query("pageSize"));

  let resolved;
  try {
    resolved = await resolveSafePath(requestPath, session.rootReal);
  } catch (error) {
    return c.json({ error: "Path not found." }, 404);
  }

  let stats;
  try {
    stats = await fs.stat(resolved.fullPath);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  if (!stats.isDirectory()) {
    return c.json({ error: "Path is not a directory." }, 400);
  }

  const dirents = await fs.readdir(resolved.fullPath, { withFileTypes: true });
  const entries = await Promise.all(
    dirents.map(async (dirent) => {
      if (dirent.isSymbolicLink()) {
        return null;
      }
      if (resolved.normalized === "/" && dirent.name === ".trash") {
        return null;
      }

      const entryPath = path.join(resolved.fullPath, dirent.name);
      try {
        const entryStat = await fs.stat(entryPath);
        return {
          name: dirent.name,
          type: dirent.isDirectory() ? "dir" : "file",
          size: entryStat.isFile() ? entryStat.size : 0,
          mtime: entryStat.mtimeMs,
        };
      } catch {
        return null;
      }
    })
  );

  const filtered = entries.filter(Boolean) as Array<{
    name: string;
    type: "dir" | "file";
    size: number;
    mtime: number;
  }>;

  filtered.sort((a, b) => {
    if (a.type !== b.type) {
      return a.type === "dir" ? -1 : 1;
    }
    return a.name.localeCompare(b.name, undefined, { sensitivity: "base" });
  });

  const totalEntries = filtered.length;
  let pagedEntries = filtered;
  let page = 1;

  if (pageSize !== null) {
    const totalPages = Math.max(1, Math.ceil(totalEntries / pageSize));
    page = Math.min(Math.max(requestedPage ?? 1, 1), totalPages);
    const startIndex = (page - 1) * pageSize;
    pagedEntries = filtered.slice(startIndex, startIndex + pageSize);
  }

  const parent = resolved.normalized === "/" ? null : path.posix.dirname(resolved.normalized);

  await auditLog(c, "list", { path: resolved.normalized, username: session.user });

  const response: {
    path: string;
    parent: string | null;
    entries: typeof filtered;
    user: string;
    role: UserRole;
    total?: number;
    page?: number;
    pageSize?: number;
  } = {
    path: resolved.normalized,
    parent,
    entries: pagedEntries,
    user: session.user,
    role: session.role,
  };

  if (pageSize !== null) {
    response.total = totalEntries;
    response.page = page;
    response.pageSize = pageSize;
  }

  return c.json(response);
});

app.get("/api/storage", async (c) => {
  const session = c.get("session");
  if (STORAGE_MODE === "s3") {
    return handleS3Storage(c, session);
  }
  const stats = await getLocalStorageStats(session.rootReal);
  return c.json(stats);
});

app.get("/api/search", async (c) => {
  const session = c.get("session");
  if (STORAGE_MODE === "s3") {
    return handleS3Search(c, session);
  }
  const requestPath = c.req.query("path") ?? "/";
  const query = (c.req.query("query") ?? "").trim();

  if (!query) {
    return c.json({ matches: [] });
  }

  let resolved;
  try {
    resolved = await resolveSafePath(requestPath, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  let stats;
  try {
    stats = await fs.stat(resolved.fullPath);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  if (!stats.isDirectory()) {
    return c.json({ error: "Path is not a directory." }, 400);
  }

  const dirents = await fs.readdir(resolved.fullPath, { withFileTypes: true });
  const needle = query.toLowerCase();
  const matches: Array<{ name: string }> = [];

  for (const dirent of dirents) {
    if (dirent.isSymbolicLink() || !dirent.isFile()) {
      continue;
    }

    const entryPath = path.join(resolved.fullPath, dirent.name);
    let entryStat;
    try {
      entryStat = await fs.stat(entryPath);
    } catch {
      continue;
    }

    if (!entryStat.isFile() || entryStat.size > MAX_SEARCH_BYTES) {
      continue;
    }

    let content: string;
    try {
      content = await Bun.file(entryPath).text();
    } catch {
      continue;
    }

    if (content.includes("\0")) {
      continue;
    }

    if (content.toLowerCase().includes(needle)) {
      matches.push({ name: dirent.name });
    }
  }

  await auditLog(c, "search", {
    path: resolved.normalized,
    username: session.user,
    query,
    matches: matches.length,
  });

  return c.json({ matches });
});

app.get("/api/image", async (c) => {
  const session = c.get("session");
  if (STORAGE_MODE === "s3") {
    return handleS3Image(c, session);
  }
  const requestPath = c.req.query("path");
  if (!requestPath) {
    return c.json({ error: "Path is required." }, 400);
  }

  let resolved;
  try {
    resolved = await resolveSafePath(requestPath, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  const stats = await fs.stat(resolved.fullPath);
  if (!stats.isFile()) {
    return c.json({ error: "Path is not a file." }, 400);
  }

  if (!isImagePreviewable(resolved.fullPath)) {
    return c.json({ error: "Image preview not available." }, 400);
  }

  const file = Bun.file(resolved.fullPath);
  const ext = path.extname(resolved.fullPath).toLowerCase();
  const mime = file.type || IMAGE_MIME_BY_EXT[ext] || "application/octet-stream";
  const filename = path.basename(resolved.fullPath);
  c.header("Content-Type", mime);
  c.header("Content-Disposition", formatContentDisposition("inline", filename));

  await auditLog(c, "image_preview", { path: resolved.normalized, username: session.user });

  return c.body(file);
});

app.get("/api/edit", async (c) => {
  const session = c.get("session");
  if (STORAGE_MODE === "s3") {
    return handleS3EditOpen(c, session);
  }
  const requestPath = c.req.query("path");
  if (!requestPath) {
    return c.json({ error: "Path is required." }, 400);
  }

  let resolved;
  try {
    resolved = await resolveSafePath(requestPath, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  const stats = await fs.stat(resolved.fullPath);
  if (!stats.isFile()) {
    return c.json({ error: "Path is not a file." }, 400);
  }

  if (!isTextEditable(resolved.fullPath)) {
    return c.json({ error: "Editor not available for this file type." }, 400);
  }

  if (stats.size > MAX_EDIT_BYTES) {
    return c.json({ error: "File is too large to edit." }, 413);
  }

  const file = Bun.file(resolved.fullPath);
  const text = await file.text();

  await auditLog(c, "edit_open", { path: resolved.normalized, username: session.user });

  return c.json({
    name: path.basename(resolved.fullPath),
    size: stats.size,
    mtime: stats.mtimeMs,
    path: resolved.normalized,
    content: text,
  });
});

app.get("/api/preview", async (c) => {
  const session = c.get("session");
  if (STORAGE_MODE === "s3") {
    return handleS3Preview(c, session);
  }
  const requestPath = c.req.query("path");
  if (!requestPath) {
    return c.json({ error: "Path is required." }, 400);
  }

  let resolved;
  try {
    resolved = await resolveSafePath(requestPath, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  const stats = await fs.stat(resolved.fullPath);
  if (!stats.isFile()) {
    return c.json({ error: "Path is not a file." }, 400);
  }

  if (!isTextPreviewable(resolved.fullPath)) {
    return c.json({ error: "Preview not available for this file type." }, 400);
  }

  if (stats.size > MAX_PREVIEW_BYTES) {
    return c.json({ error: "File is too large to preview." }, 413);
  }

  const file = Bun.file(resolved.fullPath);
  const text = await file.text();

  await auditLog(c, "preview", { path: resolved.normalized, username: session.user });

  return c.json({
    name: path.basename(resolved.fullPath),
    size: stats.size,
    mtime: stats.mtimeMs,
    content: text,
  });
});

app.post("/api/edit", async (c) => {
  const session = c.get("session");
  if (STORAGE_MODE === "s3") {
    return handleS3EditSave(c, session);
  }
  if (!canWrite(session.role)) {
    return c.json({ error: "Read-only account." }, 403);
  }

  const body = await readJsonBody<{ path?: string; content?: string }>(c);
  if (!body.path) {
    return c.json({ error: "Path is required." }, 400);
  }
  if (typeof body.content !== "string") {
    return c.json({ error: "Content is required." }, 400);
  }

  let resolved;
  try {
    resolved = await resolveSafePath(body.path, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  const stats = await fs.stat(resolved.fullPath);
  if (!stats.isFile()) {
    return c.json({ error: "Path is not a file." }, 400);
  }

  if (!isTextEditable(resolved.fullPath)) {
    return c.json({ error: "Editor not available for this file type." }, 400);
  }

  const bytes = Buffer.byteLength(body.content, "utf8");
  if (bytes > MAX_EDIT_BYTES) {
    return c.json({ error: "File is too large to save." }, 413);
  }

  await fs.writeFile(resolved.fullPath, body.content, "utf8");

  await auditLog(c, "edit_save", { path: resolved.normalized, username: session.user, bytes });

  return c.json({ ok: true });
});

app.get("/api/download", async (c) => {
  const session = c.get("session");
  if (STORAGE_MODE === "s3") {
    return handleS3Download(c, session);
  }
  const requestPath = c.req.query("path");
  if (!requestPath) {
    return c.json({ error: "Path is required." }, 400);
  }

  let resolved;
  try {
    resolved = await resolveSafePath(requestPath, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  const stats = await fs.stat(resolved.fullPath);
  if (!stats.isFile()) {
    return c.json({ error: "Path is not a file." }, 400);
  }

  const file = Bun.file(resolved.fullPath);
  const filename = path.basename(resolved.fullPath);

  c.header("Content-Type", file.type || "application/octet-stream");
  c.header("Content-Disposition", formatContentDisposition("attachment", filename));

  await auditLog(c, "download", { path: resolved.normalized, username: session.user });

  return c.body(file);
});

app.post("/api/share", async (c) => {
  const session = c.get("session");
  const body = await readJsonBody<{ path?: string; force?: boolean }>(c);
  if (!body?.path) {
    return c.json({ error: "Path is required." }, 400);
  }
  const force = Boolean(body.force);

  if (STORAGE_MODE === "s3") {
    let resolved;
    try {
      resolved = resolveSafeS3Path(body.path, session.rootReal);
    } catch {
      return c.json({ error: "Path not found." }, 404);
    }

    const info = await getS3PathInfo(session.rootReal, resolved.normalized);
    if (info.type === "none") {
      return c.json({ error: "Path not found." }, 404);
    }
    if (info.type !== "file") {
      return c.json({ error: "Path is not a file." }, 400);
    }

    if (!force) {
      const existing = await findShareForPath(resolved.normalized, session.rootReal);
      if (existing) {
        return c.json({ token: existing.token });
      }
    } else {
      await removeSharesForPath(resolved.normalized, session.rootReal);
    }

    const token = randomUUID();
    await storeShare({
      token,
      path: resolved.normalized,
      rootReal: session.rootReal,
      createdAt: Date.now(),
    });

    await auditLog(c, "share_create", {
      path: resolved.normalized,
      username: session.user,
      storage: "s3",
    });

    return c.json({ token });
  }

  let resolved;
  try {
    resolved = await resolveSafePath(body.path, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  const stats = await fs.stat(resolved.fullPath);
  if (!stats.isFile()) {
    return c.json({ error: "Path is not a file." }, 400);
  }

  if (!force) {
    const existing = await findShareForPath(resolved.normalized, session.rootReal);
    if (existing) {
      return c.json({ token: existing.token });
    }
  } else {
    await removeSharesForPath(resolved.normalized, session.rootReal);
  }

  const token = randomUUID();
  await storeShare({
    token,
    path: resolved.normalized,
    rootReal: session.rootReal,
    createdAt: Date.now(),
  });

  await auditLog(c, "share_create", {
    path: resolved.normalized,
    username: session.user,
    storage: "local",
  });

  return c.json({ token });
});

app.get("/api/share", async (c) => {
  const session = c.get("session");
  const requestPath = c.req.query("path");
  if (!requestPath) {
    return c.json({ error: "Path is required." }, 400);
  }

  if (STORAGE_MODE === "s3") {
    let resolved;
    try {
      resolved = resolveSafeS3Path(requestPath, session.rootReal);
    } catch {
      return c.json({ error: "Path not found." }, 404);
    }

    const info = await getS3PathInfo(session.rootReal, resolved.normalized);
    if (info.type !== "file") {
      return c.json({ error: "Path not found." }, 404);
    }

    const existing = await findShareForPath(resolved.normalized, session.rootReal);
    return c.json({ token: existing?.token ?? null });
  }

  let resolved;
  try {
    resolved = await resolveSafePath(requestPath, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  const stats = await fs.stat(resolved.fullPath);
  if (!stats.isFile()) {
    return c.json({ error: "Path not found." }, 404);
  }

  const existing = await findShareForPath(resolved.normalized, session.rootReal);
  return c.json({ token: existing?.token ?? null });
});

app.get("/api/share/:token", async (c) => {
  const token = c.req.param("token");
  const record = getShareRecord(token);
  if (!record) {
    return c.json({ error: "Share not found." }, 404);
  }

  if (STORAGE_MODE === "s3") {
    let resolved;
    try {
      resolved = resolveSafeS3Path(record.path, record.rootReal);
    } catch {
      return c.json({ error: "Share not found." }, 404);
    }

    const info = await getS3PathInfo(record.rootReal, resolved.normalized);
    if (info.type !== "file") {
      return c.json({ error: "Share not found." }, 404);
    }

    await auditLog(c, "share_view", { path: resolved.normalized, token, storage: "s3" });

    return c.json({
      token,
      name: path.posix.basename(resolved.normalized),
      size: info.size,
      mtime: info.mtime,
      canTextPreview: isTextPreviewable(resolved.normalized),
      canImagePreview: isImagePreviewable(resolved.normalized),
    });
  }

  let resolved;
  try {
    resolved = await resolveSafePath(record.path, record.rootReal);
  } catch {
    return c.json({ error: "Share not found." }, 404);
  }

  const stats = await fs.stat(resolved.fullPath);
  if (!stats.isFile()) {
    return c.json({ error: "Share not found." }, 404);
  }

  await auditLog(c, "share_view", { path: resolved.normalized, token, storage: "local" });

  return c.json({
    token,
    name: path.basename(resolved.fullPath),
    size: stats.size,
    mtime: stats.mtimeMs,
    canTextPreview: isTextPreviewable(resolved.fullPath),
    canImagePreview: isImagePreviewable(resolved.fullPath),
  });
});

app.get("/api/share/:token/download", async (c) => {
  const token = c.req.param("token");
  const record = getShareRecord(token);
  if (!record) {
    return c.json({ error: "Share not found." }, 404);
  }

  if (STORAGE_MODE === "s3") {
    let resolved;
    try {
      resolved = resolveSafeS3Path(record.path, record.rootReal);
    } catch {
      return c.json({ error: "Share not found." }, 404);
    }

    const info = await getS3PathInfo(record.rootReal, resolved.normalized);
    if (info.type !== "file") {
      return c.json({ error: "Share not found." }, 404);
    }

    const key = toS3Key(record.rootReal, resolved.normalized);
    const object = await s3GetObject(key);
    const filename = path.posix.basename(resolved.normalized);
    c.header("Content-Type", object.ContentType || "application/octet-stream");
    c.header("Content-Disposition", formatContentDisposition("attachment", filename));

    await auditLog(c, "share_download", { path: resolved.normalized, token, storage: "s3" });

    return c.body(object.Body as BodyInit);
  }

  let resolved;
  try {
    resolved = await resolveSafePath(record.path, record.rootReal);
  } catch {
    return c.json({ error: "Share not found." }, 404);
  }

  const stats = await fs.stat(resolved.fullPath);
  if (!stats.isFile()) {
    return c.json({ error: "Share not found." }, 404);
  }

  const file = Bun.file(resolved.fullPath);
  const filename = path.basename(resolved.fullPath);
  c.header("Content-Type", file.type || "application/octet-stream");
  c.header("Content-Disposition", formatContentDisposition("attachment", filename));

  await auditLog(c, "share_download", { path: resolved.normalized, token, storage: "local" });

  return c.body(file);
});

app.get("/api/share/:token/file", async (c) => {
  const token = c.req.param("token");
  const record = getShareRecord(token);
  if (!record) {
    return c.json({ error: "Share not found." }, 404);
  }

  if (STORAGE_MODE === "s3") {
    let resolved;
    try {
      resolved = resolveSafeS3Path(record.path, record.rootReal);
    } catch {
      return c.json({ error: "Share not found." }, 404);
    }

    const info = await getS3PathInfo(record.rootReal, resolved.normalized);
    if (info.type !== "file") {
      return c.json({ error: "Share not found." }, 404);
    }

    const key = toS3Key(record.rootReal, resolved.normalized);
    const object = await s3GetObject(key);
    const filename = path.posix.basename(resolved.normalized);
    c.header("Content-Type", object.ContentType || "application/octet-stream");
    c.header("Content-Disposition", formatContentDisposition("inline", filename));

    await auditLog(c, "share_file", { path: resolved.normalized, token, storage: "s3" });

    return c.body(object.Body as BodyInit);
  }

  let resolved;
  try {
    resolved = await resolveSafePath(record.path, record.rootReal);
  } catch {
    return c.json({ error: "Share not found." }, 404);
  }

  const stats = await fs.stat(resolved.fullPath);
  if (!stats.isFile()) {
    return c.json({ error: "Share not found." }, 404);
  }

  const file = Bun.file(resolved.fullPath);
  const filename = path.basename(resolved.fullPath);
  c.header("Content-Type", file.type || "application/octet-stream");
  c.header("Content-Disposition", formatContentDisposition("inline", filename));

  await auditLog(c, "share_file", { path: resolved.normalized, token, storage: "local" });

  return c.body(file);
});

app.get("/api/share/:token/preview", async (c) => {
  const token = c.req.param("token");
  const record = getShareRecord(token);
  if (!record) {
    return c.json({ error: "Share not found." }, 404);
  }

  if (STORAGE_MODE === "s3") {
    let resolved;
    try {
      resolved = resolveSafeS3Path(record.path, record.rootReal);
    } catch {
      return c.json({ error: "Share not found." }, 404);
    }

    const info = await getS3PathInfo(record.rootReal, resolved.normalized);
    if (info.type !== "file") {
      return c.json({ error: "Share not found." }, 404);
    }
    if (!isTextPreviewable(resolved.normalized)) {
      return c.json({ error: "Preview not available for this file type." }, 400);
    }
    if (info.size > MAX_PREVIEW_BYTES) {
      return c.json({ error: "File is too large to preview." }, 413);
    }

    const key = toS3Key(record.rootReal, resolved.normalized);
    const text = await s3ReadObjectText(key);

    await auditLog(c, "share_preview", { path: resolved.normalized, token, storage: "s3" });

    return c.json({
      name: path.posix.basename(resolved.normalized),
      size: info.size,
      mtime: info.mtime,
      content: text,
    });
  }

  let resolved;
  try {
    resolved = await resolveSafePath(record.path, record.rootReal);
  } catch {
    return c.json({ error: "Share not found." }, 404);
  }

  const stats = await fs.stat(resolved.fullPath);
  if (!stats.isFile()) {
    return c.json({ error: "Share not found." }, 404);
  }
  if (!isTextPreviewable(resolved.fullPath)) {
    return c.json({ error: "Preview not available for this file type." }, 400);
  }
  if (stats.size > MAX_PREVIEW_BYTES) {
    return c.json({ error: "File is too large to preview." }, 413);
  }

  const file = Bun.file(resolved.fullPath);
  const text = await file.text();

  await auditLog(c, "share_preview", { path: resolved.normalized, token, storage: "local" });

  return c.json({
    name: path.basename(resolved.fullPath),
    size: stats.size,
    mtime: stats.mtimeMs,
    content: text,
  });
});

app.get("/api/share/:token/image", async (c) => {
  const token = c.req.param("token");
  const record = getShareRecord(token);
  if (!record) {
    return c.json({ error: "Share not found." }, 404);
  }

  if (STORAGE_MODE === "s3") {
    let resolved;
    try {
      resolved = resolveSafeS3Path(record.path, record.rootReal);
    } catch {
      return c.json({ error: "Share not found." }, 404);
    }

    const info = await getS3PathInfo(record.rootReal, resolved.normalized);
    if (info.type !== "file") {
      return c.json({ error: "Share not found." }, 404);
    }
    if (!isImagePreviewable(resolved.normalized)) {
      return c.json({ error: "Image preview not available." }, 400);
    }

    const key = toS3Key(record.rootReal, resolved.normalized);
    const object = await s3GetObject(key);
    const ext = path.extname(resolved.normalized).toLowerCase();
    const mime = object.ContentType || IMAGE_MIME_BY_EXT[ext] || "application/octet-stream";
    const filename = path.posix.basename(resolved.normalized);
    c.header("Content-Type", mime);
    c.header("Content-Disposition", formatContentDisposition("inline", filename));

    await auditLog(c, "share_image", { path: resolved.normalized, token, storage: "s3" });

    return c.body(object.Body as BodyInit);
  }

  let resolved;
  try {
    resolved = await resolveSafePath(record.path, record.rootReal);
  } catch {
    return c.json({ error: "Share not found." }, 404);
  }

  const stats = await fs.stat(resolved.fullPath);
  if (!stats.isFile()) {
    return c.json({ error: "Share not found." }, 404);
  }
  if (!isImagePreviewable(resolved.fullPath)) {
    return c.json({ error: "Image preview not available." }, 400);
  }

  const file = Bun.file(resolved.fullPath);
  const ext = path.extname(resolved.fullPath).toLowerCase();
  const mime = file.type || IMAGE_MIME_BY_EXT[ext] || "application/octet-stream";
  const filename = path.basename(resolved.fullPath);
  c.header("Content-Type", mime);
  c.header("Content-Disposition", formatContentDisposition("inline", filename));

  await auditLog(c, "share_image", { path: resolved.normalized, token, storage: "local" });

  return c.body(file);
});

app.post("/api/mkdir", async (c) => {
  const session = c.get("session");
  if (STORAGE_MODE === "s3") {
    return handleS3Mkdir(c, session);
  }
  if (!canWrite(session.role)) {
    return c.json({ error: "Read-only account." }, 403);
  }

  const body = await readJsonBody<{ path?: string; name?: string }>(c);
  if (!body) {
    return c.json({ error: "Invalid JSON." }, 400);
  }

  const parentPath = typeof body.path === "string" ? body.path : "/";
  const name = sanitizeName(body.name ?? "");
  if (!name) {
    return c.json({ error: "Folder name is required." }, 400);
  }

  let parent;
  try {
    parent = await resolveSafePath(parentPath, session.rootReal);
  } catch {
    return c.json({ error: "Parent path not found." }, 404);
  }

  const parentStat = await fs.stat(parent.fullPath);
  if (!parentStat.isDirectory()) {
    return c.json({ error: "Parent path is not a directory." }, 400);
  }

  const fullPath = path.join(parent.fullPath, name);
  if (!isWithinRoot(fullPath, session.rootReal)) {
    return c.json({ error: "Invalid path." }, 400);
  }

  if (await pathExists(fullPath)) {
    return c.json({ error: "Folder already exists." }, 409);
  }

  await fs.mkdir(fullPath);
  const createdPath = parent.normalized === "/" ? `/${name}` : `${parent.normalized}/${name}`;
  await auditLog(c, "mkdir", { path: createdPath, username: session.user });
  return c.json({ ok: true });
});

app.post("/api/upload", async (c) => {
  const session = c.get("session");
  if (STORAGE_MODE === "s3") {
    return handleS3Upload(c, session);
  }
  if (!canWrite(session.role)) {
    return c.json({ error: "Read-only account." }, 403);
  }

  const form = await c.req.formData();
  const targetPath = form.get("path");
  if (typeof targetPath !== "string") {
    return c.json({ error: "Path is required." }, 400);
  }

  const overwrite = form.get("overwrite") === "1";

  let resolved;
  try {
    resolved = await resolveSafePath(targetPath, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  const dirStat = await fs.stat(resolved.fullPath);
  if (!dirStat.isDirectory()) {
    return c.json({ error: "Path is not a directory." }, 400);
  }

  const files = Array.from(form.entries())
    .filter(([key, value]) => key === "files" && value instanceof File)
    .map(([, value]) => value as File);

  if (files.length === 0) {
    return c.json({ error: "No files provided." }, 400);
  }

  const uploaded: string[] = [];
  for (const file of files) {
    const fileName = sanitizeName(file.name);
    if (!fileName) {
      return c.json({ error: "Invalid file name." }, 400);
    }

    const destPath = path.join(resolved.fullPath, fileName);
    if (!isWithinRoot(destPath, session.rootReal)) {
      return c.json({ error: "Invalid file path." }, 400);
    }

    if (!overwrite && (await pathExists(destPath))) {
      return c.json({ error: `File exists: ${fileName}` }, 409);
    }

    await Bun.write(destPath, file);
    uploaded.push(fileName);
  }

  await auditLog(c, "upload", {
    path: resolved.normalized,
    files: uploaded,
    username: session.user,
  });

  return c.json({ ok: true });
});

app.post("/api/upload/init", async (c) => {
  const session = c.get("session");
  if (!canWrite(session.role)) {
    return c.json({ error: "Read-only account." }, 403);
  }

  const body = await readJsonBody<{
    path?: string;
    name?: string;
    size?: number;
    totalChunks?: number;
    overwrite?: boolean;
  }>(c);
  if (!body?.path || !body.name || !Number.isFinite(body.size) || !body.totalChunks) {
    return c.json({ error: "Invalid upload init payload." }, 400);
  }

  const sanitized = sanitizeName(body.name);
  if (!sanitized) {
    return c.json({ error: "Invalid file name." }, 400);
  }

  if (STORAGE_MODE === "s3") {
    return handleS3UploadInit(c, session, {
      path: body.path,
      name: sanitized,
      size: body.size,
      totalChunks: body.totalChunks,
      overwrite: Boolean(body.overwrite),
    });
  }

  return handleLocalUploadInit(c, session, {
    path: body.path,
    name: sanitized,
    size: body.size,
    totalChunks: body.totalChunks,
    overwrite: Boolean(body.overwrite),
  });
});

app.get("/api/upload/status", async (c) => {
  const session = c.get("session");
  const uploadId = c.req.query("uploadId");
  if (!uploadId) {
    return c.json({ error: "Upload id is required." }, 400);
  }

  if (STORAGE_MODE === "s3") {
    const key = c.req.query("key");
    if (!key) {
      return c.json({ error: "Key is required." }, 400);
    }
    return handleS3UploadStatus(c, session, uploadId, key);
  }

  return handleLocalUploadStatus(c, session, uploadId);
});

app.post("/api/upload/chunk", async (c) => {
  const session = c.get("session");
  if (!canWrite(session.role)) {
    return c.json({ error: "Read-only account." }, 403);
  }
  const form = await c.req.formData();
  const uploadId = form.get("uploadId");
  const partRaw = form.get("partNumber");
  const totalRaw = form.get("totalChunks");
  const chunk = form.get("chunk");

  if (typeof uploadId !== "string" || typeof partRaw !== "string" || typeof totalRaw !== "string") {
    return c.json({ error: "Invalid upload chunk payload." }, 400);
  }
  if (!(chunk instanceof File)) {
    return c.json({ error: "Chunk is required." }, 400);
  }

  const partNumber = Number.parseInt(partRaw, 10);
  const totalChunks = Number.parseInt(totalRaw, 10);
  if (!Number.isFinite(partNumber) || partNumber <= 0 || !Number.isFinite(totalChunks)) {
    return c.json({ error: "Invalid chunk index." }, 400);
  }

  if (STORAGE_MODE === "s3") {
    const key = form.get("key");
    if (typeof key !== "string") {
      return c.json({ error: "Key is required." }, 400);
    }
    return handleS3UploadChunk(c, session, { uploadId, key, partNumber, chunk });
  }

  return handleLocalUploadChunk(c, session, { uploadId, partNumber, totalChunks, chunk });
});

app.post("/api/upload/complete", async (c) => {
  const session = c.get("session");
  if (!canWrite(session.role)) {
    return c.json({ error: "Read-only account." }, 403);
  }

  const body = await readJsonBody<{
    uploadId?: string;
    key?: string;
    totalChunks?: number;
  }>(c);
  if (!body?.uploadId || !body.totalChunks) {
    return c.json({ error: "Invalid upload completion payload." }, 400);
  }

  if (STORAGE_MODE === "s3") {
    if (!body.key) {
      return c.json({ error: "Key is required." }, 400);
    }
    return handleS3UploadComplete(c, session, body.uploadId, body.key, body.totalChunks);
  }

  return handleLocalUploadComplete(c, session, body.uploadId, body.totalChunks);
});

app.post("/api/move", async (c) => {
  const session = c.get("session");
  if (STORAGE_MODE === "s3") {
    return handleS3Move(c, session);
  }
  if (!canWrite(session.role)) {
    return c.json({ error: "Read-only account." }, 403);
  }

  const body = await readJsonBody<{ from?: string; to?: string }>(c);
  if (!body) {
    return c.json({ error: "Invalid JSON." }, 400);
  }

  if (typeof body.from !== "string" || typeof body.to !== "string") {
    return c.json({ error: "From and to paths are required." }, 400);
  }

  let fromResolved;
  try {
    fromResolved = await resolveSafePath(body.from, session.rootReal);
  } catch {
    return c.json({ error: "Source path not found." }, 404);
  }

  if (fromResolved.normalized === "/") {
    return c.json({ error: "Cannot move the root." }, 400);
  }

  const dest = await resolveDestinationPath(body.to, session.rootReal);
  if (!dest) {
    return c.json({ error: "Destination path is invalid." }, 400);
  }

  if (!isWithinRoot(dest.fullPath, session.rootReal)) {
    return c.json({ error: "Destination path is invalid." }, 400);
  }

  if (dest.fullPath === fromResolved.fullPath) {
    return c.json({ error: "Destination matches source." }, 400);
  }

  if (await pathExists(dest.fullPath)) {
    return c.json({ error: "Destination already exists." }, 409);
  }

  if (dest.fullPath.startsWith(`${fromResolved.fullPath}${path.sep}`)) {
    return c.json({ error: "Cannot move a folder into itself." }, 400);
  }

  await fs.rename(fromResolved.fullPath, dest.fullPath);
  await auditLog(c, "move", {
    from: fromResolved.normalized,
    to: dest.normalized,
    username: session.user,
  });
  return c.json({ ok: true });
});

app.post("/api/copy", async (c) => {
  const session = c.get("session");
  if (STORAGE_MODE === "s3") {
    return handleS3Copy(c, session);
  }
  if (!canWrite(session.role)) {
    return c.json({ error: "Read-only account." }, 403);
  }

  const body = await readJsonBody<{ from?: string; to?: string }>(c);
  if (!body) {
    return c.json({ error: "Invalid JSON." }, 400);
  }

  if (typeof body.from !== "string" || typeof body.to !== "string") {
    return c.json({ error: "From and to paths are required." }, 400);
  }

  let fromResolved;
  try {
    fromResolved = await resolveSafePath(body.from, session.rootReal);
  } catch {
    return c.json({ error: "Source path not found." }, 404);
  }

  const dest = await resolveDestinationPath(body.to, session.rootReal);
  if (!dest) {
    return c.json({ error: "Destination path is invalid." }, 400);
  }

  if (await pathExists(dest.fullPath)) {
    return c.json({ error: "Destination already exists." }, 409);
  }

  if (dest.fullPath.startsWith(`${fromResolved.fullPath}${path.sep}`)) {
    return c.json({ error: "Cannot copy a folder into itself." }, 400);
  }

  await copyPath(fromResolved.fullPath, dest.fullPath);
  await auditLog(c, "copy", {
    from: fromResolved.normalized,
    to: dest.normalized,
    username: session.user,
  });
  return c.json({ ok: true });
});

app.post("/api/trash", async (c) => {
  const session = c.get("session");
  if (STORAGE_MODE === "s3") {
    return handleS3Trash(c, session);
  }
  if (!canWrite(session.role)) {
    return c.json({ error: "Read-only account." }, 403);
  }

  const body = await readJsonBody<{ path?: string }>(c);
  if (!body) {
    return c.json({ error: "Invalid JSON." }, 400);
  }

  if (typeof body.path !== "string") {
    return c.json({ error: "Path is required." }, 400);
  }

  let resolved;
  try {
    resolved = await resolveSafePath(body.path, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  if (resolved.normalized === "/") {
    return c.json({ error: "Cannot delete the root." }, 400);
  }

  const stats = await fs.stat(resolved.fullPath);

  const { trashDir, metaDir } = getTrashPaths(session.rootReal);
  await ensureTrashDirs(metaDir);
  const id = randomUUID();
  const fileName = sanitizeName(path.basename(resolved.fullPath)) ?? "item";
  const trashName = `${Date.now()}-${fileName}-${id}`;
  const trashPath = path.join(trashDir, trashName);

  await fs.rename(resolved.fullPath, trashPath);

  const record: TrashRecord = {
    id,
    name: fileName,
    originalPath: resolved.normalized,
    deletedAt: Date.now(),
    type: stats.isDirectory() ? "dir" : "file",
    size: stats.isFile() ? stats.size : 0,
    trashName,
  };

  await fs.writeFile(path.join(metaDir, `${id}.json`), JSON.stringify(record));
  await auditLog(c, "trash", {
    path: resolved.normalized,
    username: session.user,
  });

  return c.json({ ok: true, item: record });
});

app.get("/api/trash", async (c) => {
  const session = c.get("session");
  if (STORAGE_MODE === "s3") {
    return handleS3TrashList(c, session);
  }
  const { metaDir } = getTrashPaths(session.rootReal);
  await ensureTrashDirs(metaDir);
  const records = await readTrashRecords(metaDir);
  await auditLog(c, "trash_list", { username: session.user });
  return c.json({ items: records, user: session.user, role: session.role });
});

app.post("/api/trash/restore", async (c) => {
  const session = c.get("session");
  if (STORAGE_MODE === "s3") {
    return handleS3TrashRestore(c, session);
  }
  if (!canWrite(session.role)) {
    return c.json({ error: "Read-only account." }, 403);
  }

  const body = await readJsonBody<{ id?: string }>(c);
  if (!body) {
    return c.json({ error: "Invalid JSON." }, 400);
  }

  if (typeof body.id !== "string" || !body.id) {
    return c.json({ error: "Trash id is required." }, 400);
  }

  const { trashDir, metaDir } = getTrashPaths(session.rootReal);
  await ensureTrashDirs(metaDir);
  const record = await readTrashRecord(metaDir, body.id);
  if (!record) {
    return c.json({ error: "Trash record not found." }, 404);
  }

  const normalized = normalizeRequestPath(record.originalPath);
  if (normalized === "/" || normalized.startsWith("/.trash")) {
    return c.json({ error: "Invalid restore path." }, 400);
  }

  const parentNormalized = path.posix.dirname(normalized);
  let parent;
  try {
    parent = await resolveSafePath(parentNormalized, session.rootReal);
  } catch {
    return c.json({ error: "Restore location no longer exists." }, 409);
  }

  const destPath = path.join(parent.fullPath, path.posix.basename(normalized));
  if (await pathExists(destPath)) {
    return c.json({ error: "Restore target already exists." }, 409);
  }

  const trashPath = path.join(trashDir, record.trashName);
  if (!(await pathExists(trashPath))) {
    return c.json({ error: "Trash item not found." }, 404);
  }

  await fs.rename(trashPath, destPath);
  await fs.unlink(path.join(metaDir, `${record.id}.json`));
  await auditLog(c, "restore", {
    path: normalized,
    username: session.user,
  });

  return c.json({ ok: true });
});

app.get("/api/archive", async (c) => {
  const session = c.get("session");
  if (STORAGE_MODE === "s3") {
    return handleS3Archive(c, session);
  }
  const url = new URL(c.req.url);
  const requested = url.searchParams.getAll("path");
  if (requested.length === 0) {
    return c.json({ error: "No paths provided." }, 400);
  }

  const formatParam = url.searchParams.get("format");
  const formatRaw = formatParam ? formatParam.toLowerCase() : "zip";
  const format =
    formatRaw === "zip"
      ? "zip"
      : formatRaw === "targz" || formatRaw === "tar.gz" || formatRaw === "tgz"
        ? "targz"
        : null;
  if (!format) {
    return c.json({ error: "Invalid archive format." }, 400);
  }

  const resolved: string[] = [];
  const resolvedFullPaths: string[] = [];
  const isSingle = requested.length === 1;
  let singleName: string | null = null;
  const archiveRoot = session.rootReal;
  for (const item of requested) {
    let resolvedItem;
    try {
      resolvedItem = await resolveSafePath(item, archiveRoot);
    } catch {
      return c.json({ error: "Path not found." }, 404);
    }

    if (resolvedItem.normalized === "/") {
      return c.json({ error: "Cannot archive the root." }, 400);
    }

    resolved.push(path.relative(archiveRoot, resolvedItem.fullPath));
    resolvedFullPaths.push(resolvedItem.fullPath);
    if (isSingle) {
      singleName = path.basename(resolvedItem.fullPath);
    }
  }

  if (resolved.length === 0) {
    return c.json({ error: "No valid paths provided." }, 400);
  }

  const totalBytes =
    format === "zip"
      ? await getArchiveTotalBytes(resolvedFullPaths, ARCHIVE_LARGE_BYTES)
      : 0;
  const useStore = format === "zip" && totalBytes >= ARCHIVE_LARGE_BYTES;
  const compression = format === "zip" ? (useStore ? "store" : "normal") : "gzip";

  const timestamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
  const baseName = singleName ?? `bundle-${timestamp}`;
  const safeBaseName = baseName.replace(/[\r\n"]/g, "") || "bundle";
  const archiveName = format === "zip" ? `${safeBaseName}.zip` : `${safeBaseName}.tar.gz`;

  let process;
  try {
    const cmd =
      format === "zip"
        ? ["zip", "-q", "-r", "-y", ...(useStore ? ["-0"] : []), "-", ...resolved]
        : ["tar", "-czf", "-", ...resolved];
    process = Bun.spawn({
      cmd,
      cwd: archiveRoot,
      stdout: "pipe",
      stderr: "pipe",
    });
  } catch {
    return c.json({ error: "Archive tool is not available." }, 500);
  }

  process.exited.then((code) => {
    if (code !== 0) {
      process.stderr
        ?.text()
        .then((text) => console.error("Archive failed:", text.trim()))
        .catch(() => {});
    }
  });

  c.header("Content-Type", format === "zip" ? "application/zip" : "application/gzip");
  c.header("Content-Disposition", formatContentDisposition("attachment", archiveName));
  await auditLog(c, "archive", { paths: requested, format, compression, username: session.user });
  return c.body(process.stdout);
});

app.use("/*", serveStatic({ root: "./dist" }));

app.get("/*", async (c) => {
  if (c.req.path.startsWith("/api/")) {
    return c.json({ error: "Not found." }, 404);
  }

  const file = Bun.file("./dist/index.html");
  if (!(await file.exists())) {
    return c.text("Frontend build not found. Run 'bun run build' from the repo root.", 500);
  }

  return c.html(await file.text());
});

app.onError((err, c) => {
  console.error(err);
  return c.json({ error: "Internal server error." }, 500);
});

const PORT = process.env.PORT === undefined ? 3033 : Number(process.env.PORT);
const PORT_VALUE = Number.isNaN(PORT) ? 3033 : PORT;

Bun.serve({
  fetch: app.fetch,
  port: PORT_VALUE,
});

function safeEqual(a: string, b: string) {
  if (a.length !== b.length) {
    return false;
  }
  return timingSafeEqual(Buffer.from(a), Buffer.from(b));
}

function createSession(user: string) {
  const payload: SessionPayload = {
    exp: Date.now() + SESSION_TTL_MS,
    nonce: randomUUID(),
    user,
  };
  const encoded = base64UrlEncode(JSON.stringify(payload));
  const signature = sign(encoded);
  return `${encoded}.${signature}`;
}

function verifySession(token?: string): SessionPayload | null {
  if (!token) {
    return null;
  }

  const [payload, signature] = token.split(".");
  if (!payload || !signature) {
    return null;
  }

  const expected = sign(payload);
  if (!safeEqual(expected, signature)) {
    return null;
  }

  const decoded = base64UrlDecode(payload);
  if (!decoded) {
    return null;
  }

  try {
    const data = JSON.parse(decoded) as { exp?: number; nonce?: string; user?: string };
    if (
      typeof data.exp !== "number" ||
      typeof data.nonce !== "string" ||
      typeof data.user !== "string"
    ) {
      return null;
    }
    if (data.exp <= Date.now()) {
      return null;
    }
    return { exp: data.exp, nonce: data.nonce, user: data.user };
  } catch {
    return null;
  }
}

function base64UrlEncode(value: string) {
  return Buffer.from(value).toString("base64url");
}

function base64UrlDecode(value: string) {
  try {
    return Buffer.from(value, "base64url").toString("utf8");
  } catch {
    return null;
  }
}

function sign(value: string) {
  return createHmac("sha256", SESSION_SECRET).update(value).digest("base64url");
}

function isSecureRequest(c: Parameters<typeof setCookie>[0]) {
  const forwardedProto = c.req.header("x-forwarded-proto");
  if (forwardedProto) {
    return forwardedProto.split(",")[0]?.trim() === "https";
  }
  return c.req.url.startsWith("https://");
}

function setSessionCookie(c: Parameters<typeof setCookie>[0], value: string) {
  setCookie(c, SESSION_COOKIE, value, {
    ...SESSION_COOKIE_BASE_OPTIONS,
    secure: isSecureRequest(c),
  });
}

function canWrite(role: UserRole) {
  return role !== "read-only";
}

function resolveLoginUser(username: string) {
  if (username) {
    const direct = USER_MAP.get(username);
    if (direct) {
      return direct;
    }
  }
  if (USER_MAP.size === 1) {
    return USER_MAP.values().next().value ?? null;
  }
  return null;
}

function verifyUserPassword(user: UserRecord, password: string) {
  if (user.passwordHash) {
    return verifyPasswordHash(user.passwordHash, password);
  }
  if (typeof user.password === "string") {
    return safeEqual(user.password, password);
  }
  return false;
}

function verifyPasswordHash(hash: string, password: string) {
  if (!hash.startsWith("scrypt$")) {
    return false;
  }
  const [, saltB64, hashB64] = hash.split("$");
  if (!saltB64 || !hashB64) {
    return false;
  }
  const salt = Buffer.from(saltB64, "base64");
  const expected = Buffer.from(hashB64, "base64");
  const derived = scryptSync(password, salt, expected.length);
  return timingSafeEqual(expected, derived);
}

function normalizeUserRole(role?: string): UserRole {
  const normalized = (role ?? "read-write").toLowerCase();
  if (normalized === "admin" || normalized === "read-only" || normalized === "read-write") {
    return normalized;
  }
  throw new Error(`Invalid role: ${role}`);
}

async function loadUsers(): Promise<UserRecord[]> {
  let configs: UserConfig[] = [];

  if (USERS_JSON) {
    configs = JSON.parse(USERS_JSON) as UserConfig[];
  } else if (USERS_FILE) {
    const raw = await fs.readFile(USERS_FILE, "utf8");
    configs = JSON.parse(raw) as UserConfig[];
  } else if (PASSWORD) {
    configs = [
      {
        username: "admin",
        password: PASSWORD,
        role: "admin",
        root: "/",
      },
    ];
  }

  if (!Array.isArray(configs) || configs.length === 0) {
    return [];
  }

  const records: UserRecord[] = [];
  const seen = new Set<string>();
  for (const config of configs) {
    const username = (config.username ?? "").trim();
    if (!username) {
      throw new Error("User is missing a username.");
    }
    if (seen.has(username)) {
      throw new Error(`Duplicate username: ${username}`);
    }
    seen.add(username);

    const role = normalizeUserRole(config.role);
    const root = config.root ?? "/";
    const { rootPath, rootReal } = await resolveUserRoot(root);

    if (!config.password && !config.passwordHash) {
      throw new Error(`User ${username} is missing a password.`);
    }

    records.push({
      username,
      role,
      rootPath,
      rootReal,
      password: config.password,
      passwordHash: config.passwordHash,
    });
  }

  return records;
}

async function resolveUserRoot(rootPath: string) {
  if (STORAGE_MODE === "s3") {
    return resolveUserRootS3(rootPath);
  }
  const normalized = normalizeRequestPath(rootPath);
  if (normalized === "/.trash" || normalized.startsWith("/.trash/")) {
    throw new Error("User root cannot be .trash");
  }
  const joined = path.resolve(ROOT_REAL, `.${normalized}`);
  const real = await fs.realpath(joined);
  if (!isWithinRoot(real, ROOT_REAL)) {
    throw new Error("User root escapes FILE_ROOT");
  }
  const stats = await fs.stat(real);
  if (!stats.isDirectory()) {
    throw new Error("User root must be a directory");
  }
  return { rootPath: normalized, rootReal: real };
}

function getTrashPaths(rootReal: string) {
  const trashDir = path.join(rootReal, ".trash");
  const metaDir = path.join(trashDir, ".meta");
  return { trashDir, metaDir };
}

function getRequestIp(c: Parameters<typeof setCookie>[0]) {
  const forwarded = c.req.header("x-forwarded-for");
  if (forwarded) {
    return forwarded.split(",")[0]?.trim() || "unknown";
  }
  return c.req.header("x-real-ip") ?? "unknown";
}

async function auditLog(
  c: Parameters<typeof setCookie>[0],
  action: string,
  meta: Record<string, unknown>
) {
  const record = {
    ts: new Date().toISOString(),
    ip: getRequestIp(c),
    action,
    ...meta,
  };
  try {
    await fs.appendFile(AUDIT_LOG_PATH, `${JSON.stringify(record)}\n`);
  } catch (error) {
    console.error("Audit log failed:", error);
  }
}

async function loadShareStore() {
  let raw: string;
  try {
    raw = await fs.readFile(SHARE_STORE_PATH, "utf8");
  } catch {
    return;
  }

  try {
    const data = JSON.parse(raw);
    if (!Array.isArray(data)) {
      return;
    }
    let deduped = false;
    for (const item of data) {
      if (
        item &&
        typeof item.token === "string" &&
        typeof item.path === "string" &&
        typeof item.rootReal === "string" &&
        typeof item.createdAt === "number"
      ) {
        const record = {
          token: item.token,
          path: item.path,
          rootReal: item.rootReal,
          createdAt: item.createdAt,
        };
        SHARE_MAP.set(record.token, record);
        if (!record.rootReal) {
          deduped = true;
        }
      }
    }
    if (deduped) {
      await saveShareStore();
    }
  } catch (error) {
    console.error("Share store failed to load:", error);
  }
}

async function saveShareStore() {
  const deduped = new Map<string, ShareRecord>();
  for (const record of SHARE_MAP.values()) {
    const key = `${record.rootReal}::${record.path}`;
    const existing = deduped.get(key);
    if (!existing || record.createdAt > existing.createdAt) {
      deduped.set(key, record);
    }
  }
  const records = Array.from(deduped.values()).map((record) => ({
    token: record.token,
    path: record.path,
    rootReal: record.rootReal,
    createdAt: record.createdAt,
  }));
  try {
    await fs.writeFile(SHARE_STORE_PATH, JSON.stringify(records, null, 2), "utf8");
  } catch (error) {
    console.error("Share store failed to save:", error);
  }
}

async function storeShare(record: ShareRecord) {
  SHARE_MAP.set(record.token, record);
  await saveShareStore();
}

async function findShareForPath(targetPath: string, rootReal: string) {
  let match: ShareRecord | null = null;
  let needsSave = false;
  for (const record of SHARE_MAP.values()) {
    if (record.path !== targetPath) {
      continue;
    }
    if (record.rootReal && record.rootReal !== rootReal) {
      continue;
    }
    if (!record.rootReal) {
      record.rootReal = rootReal;
      SHARE_MAP.set(record.token, record);
      needsSave = true;
    }
    if (record.rootReal === rootReal) {
      if (!match || record.createdAt > match.createdAt) {
        match = record;
      }
    }
  }
  if (needsSave) {
    await saveShareStore();
  }
  return match;
}

async function removeSharesForPath(targetPath: string, rootReal: string) {
  let removed = false;
  for (const [token, record] of SHARE_MAP.entries()) {
    if (
      record.path === targetPath &&
      (!record.rootReal || record.rootReal === rootReal)
    ) {
      SHARE_MAP.delete(token);
      removed = true;
    }
  }
  if (removed) {
    await saveShareStore();
  }
}

function getShareRecord(token: string) {
  return SHARE_MAP.get(token) ?? null;
}

type TrashRecord = {
  id: string;
  name: string;
  originalPath: string;
  deletedAt: number;
  type: "dir" | "file";
  size: number;
  trashName: string;
};

async function readJsonBody<T>(c: Parameters<typeof setCookie>[0]) {
  try {
    return (await c.req.json()) as T;
  } catch {
    return null;
  }
}

function parsePositiveInt(value: string | undefined) {
  if (!value) {
    return null;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return null;
  }
  return parsed;
}

function encodeContentDispositionFilename(value: string) {
  return encodeURIComponent(value).replace(/[!'()*]/g, (char) => {
    return `%${char.charCodeAt(0).toString(16).toUpperCase()}`;
  });
}

function formatContentDisposition(type: "inline" | "attachment", filename: string) {
  const base = filename.replace(/[\r\n"]/g, "").replace(/[\\/]/g, "_");
  const fallback = base.replace(/[^\x20-\x7E]+/g, "_").trim() || "file";
  const encoded = encodeContentDispositionFilename(filename);
  return `${type}; filename="${fallback}"; filename*=UTF-8''${encoded}`;
}

function sanitizeName(value: string) {
  const trimmed = value.trim().replace(/\0/g, "");
  if (!trimmed) {
    return null;
  }
  if (trimmed.includes("/") || trimmed.includes("\\")) {
    return null;
  }
  if (trimmed === "." || trimmed === "..") {
    return null;
  }
  return trimmed;
}

function isTextPreviewable(filePath: string) {
  return TEXT_PREVIEW_EXTS.has(path.extname(filePath).toLowerCase());
}

function isTextEditable(filePath: string) {
  return TEXT_EDIT_EXTS.has(path.extname(filePath).toLowerCase());
}

function isImagePreviewable(filePath: string) {
  return IMAGE_PREVIEW_EXTS.has(path.extname(filePath).toLowerCase());
}

async function pathExists(target: string) {
  try {
    await fs.stat(target);
    return true;
  } catch {
    return false;
  }
}

async function getArchiveTotalBytes(paths: string[], limit: number) {
  if (limit <= 0) {
    return 0;
  }
  let total = 0;
  for (const item of paths) {
    if (total >= limit) {
      break;
    }
    total += await sumPathBytes(item, limit - total);
  }
  return total;
}

async function sumPathBytes(root: string, limit: number) {
  if (limit <= 0) {
    return 0;
  }
  const stack = [root];
  let total = 0;
  while (stack.length > 0) {
    const current = stack.pop();
    if (!current) {
      continue;
    }
    let stats;
    try {
      stats = await fs.lstat(current);
    } catch {
      continue;
    }
    if (stats.isSymbolicLink()) {
      continue;
    }
    if (stats.isFile()) {
      total += stats.size;
    } else if (stats.isDirectory()) {
      try {
        const dirents = await fs.readdir(current, { withFileTypes: true });
        for (const dirent of dirents) {
          if (dirent.isSymbolicLink()) {
            continue;
          }
          stack.push(path.join(current, dirent.name));
        }
      } catch {
        continue;
      }
    }
    if (total >= limit) {
      break;
    }
  }
  return total;
}

async function getLocalStorageStats(root: string) {
  const stack = [root];
  let totalBytes = 0;
  let totalFiles = 0;

  while (stack.length > 0) {
    const current = stack.pop();
    if (!current) {
      continue;
    }
    let stats;
    try {
      stats = await fs.lstat(current);
    } catch {
      continue;
    }
    if (stats.isSymbolicLink()) {
      continue;
    }
    if (stats.isFile()) {
      totalBytes += stats.size;
      totalFiles += 1;
      continue;
    }
    if (stats.isDirectory()) {
      try {
        const dirents = await fs.readdir(current, { withFileTypes: true });
        for (const dirent of dirents) {
          if (dirent.isSymbolicLink()) {
            continue;
          }
          stack.push(path.join(current, dirent.name));
        }
      } catch {
        continue;
      }
    }
  }

  return { totalBytes, totalFiles };
}

async function resolveDestinationPath(target: string, rootReal: string) {
  const normalized = normalizeRequestPath(target);
  if (normalized === "/" || normalized.startsWith("/.trash")) {
    return null;
  }
  const parentNormalized = path.posix.dirname(normalized);
  const name = sanitizeName(path.posix.basename(normalized));
  if (!name) {
    return null;
  }
  const parent = await resolveSafePath(parentNormalized, rootReal);
  const fullPath = path.join(parent.fullPath, name);
  return { normalized, fullPath };
}

async function copyPath(source: string, destination: string) {
  const stats = await fs.stat(source);
  if (stats.isDirectory()) {
    await fs.mkdir(destination);
    const dirents = await fs.readdir(source, { withFileTypes: true });
    for (const dirent of dirents) {
      if (dirent.isSymbolicLink()) {
        continue;
      }
      const src = path.join(source, dirent.name);
      const dest = path.join(destination, dirent.name);
      if (dirent.isDirectory()) {
        await copyPath(src, dest);
      } else if (dirent.isFile()) {
        await fs.copyFile(src, dest);
      }
    }
    return;
  }

  if (stats.isFile()) {
    await fs.copyFile(source, destination);
    return;
  }

  throw new Error("Unsupported file type");
}

async function ensureTrashDirs(metaDir: string) {
  await fs.mkdir(metaDir, { recursive: true });
}

async function readTrashRecord(metaDir: string, id: string): Promise<TrashRecord | null> {
  try {
    const raw = await fs.readFile(path.join(metaDir, `${id}.json`), "utf8");
    const parsed = JSON.parse(raw) as TrashRecord;
    if (!parsed?.id || !parsed?.trashName || !parsed?.originalPath) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

async function readTrashRecords(metaDir: string): Promise<TrashRecord[]> {
  const files = await fs.readdir(metaDir);
  const records: TrashRecord[] = [];
  for (const file of files) {
    if (!file.endsWith(".json")) {
      continue;
    }
    const id = file.replace(/\.json$/, "");
    const record = await readTrashRecord(metaDir, id);
    if (record) {
      records.push(record);
    }
  }
  records.sort((a, b) => b.deletedAt - a.deletedAt);
  return records;
}

function normalizeRequestPath(input: string | undefined) {
  let raw = (input ?? "/").trim();
  if (!raw) {
    raw = "/";
  }

  raw = raw.replace(/\\/g, "/");
  if (!raw.startsWith("/")) {
    raw = `/${raw}`;
  }

  const normalized = path.posix.normalize(raw);
  if (!normalized.startsWith("/")) {
    return "/";
  }

  return normalized;
}

function isWithinRoot(candidate: string, rootReal: string) {
  if (candidate === rootReal) {
    return true;
  }

  const rootWithSep = rootReal.endsWith(path.sep) ? rootReal : `${rootReal}${path.sep}`;
  return candidate.startsWith(rootWithSep);
}

async function resolveSafePath(requestPath: string, rootReal: string) {
  const normalized = normalizeRequestPath(requestPath);
  if (normalized === "/.trash" || normalized.startsWith("/.trash/")) {
    throw new Error("Path not allowed");
  }
  const joined = path.resolve(rootReal, `.${normalized}`);
  const real = await fs.realpath(joined);

  if (!isWithinRoot(real, rootReal)) {
    throw new Error("Path escapes root");
  }

  return { normalized, fullPath: real };
}

async function handleS3List(c: AppContext, session: SessionContext) {
  const requestPath = c.req.query("path") ?? "/";
  const requestedPage = parsePositiveInt(c.req.query("page"));
  const pageSize = parsePositiveInt(c.req.query("pageSize"));
  let resolved;
  try {
    resolved = resolveSafeS3Path(requestPath, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  if (resolved.normalized !== "/") {
    const info = await getS3PathInfo(session.rootReal, resolved.normalized);
    if (info.type === "none") {
      return c.json({ error: "Path not found." }, 404);
    }
    if (info.type !== "dir") {
      return c.json({ error: "Path is not a directory." }, 400);
    }
  }

  const prefix = toS3Prefix(session.rootReal, resolved.normalized);
  const entries = await listS3Entries(prefix, resolved.normalized);

  const totalEntries = entries.length;
  let pagedEntries = entries;
  let page = 1;

  if (pageSize !== null) {
    const totalPages = Math.max(1, Math.ceil(totalEntries / pageSize));
    page = Math.min(Math.max(requestedPage ?? 1, 1), totalPages);
    const startIndex = (page - 1) * pageSize;
    pagedEntries = entries.slice(startIndex, startIndex + pageSize);
  }

  const parent = resolved.normalized === "/" ? null : path.posix.dirname(resolved.normalized);

  await auditLog(c, "list", { path: resolved.normalized, username: session.user, storage: "s3" });

  const response: {
    path: string;
    parent: string | null;
    entries: typeof entries;
    user: string;
    role: UserRole;
    total?: number;
    page?: number;
    pageSize?: number;
  } = {
    path: resolved.normalized,
    parent,
    entries: pagedEntries,
    user: session.user,
    role: session.role,
  };

  if (pageSize !== null) {
    response.total = totalEntries;
    response.page = page;
    response.pageSize = pageSize;
  }

  return c.json(response);
}

async function handleS3Search(c: AppContext, session: SessionContext) {
  const requestPath = c.req.query("path") ?? "/";
  const query = (c.req.query("query") ?? "").trim();

  if (!query) {
    return c.json({ matches: [] });
  }

  let resolved;
  try {
    resolved = resolveSafeS3Path(requestPath, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  if (resolved.normalized !== "/") {
    const info = await getS3PathInfo(session.rootReal, resolved.normalized);
    if (info.type === "none") {
      return c.json({ error: "Path not found." }, 404);
    }
    if (info.type !== "dir") {
      return c.json({ error: "Path is not a directory." }, 400);
    }
  }

  const prefix = toS3Prefix(session.rootReal, resolved.normalized);
  const entries = await listS3Entries(prefix, resolved.normalized);
  const needle = query.toLowerCase();
  const matches: Array<{ name: string }> = [];

  for (const entry of entries) {
    if (entry.type !== "file") {
      continue;
    }
    if (entry.size > MAX_SEARCH_BYTES) {
      continue;
    }
    const key = `${prefix}${entry.name}`;
    let content: string;
    try {
      content = await s3ReadObjectText(key);
    } catch {
      continue;
    }
    if (content.includes("\0")) {
      continue;
    }
    if (content.toLowerCase().includes(needle)) {
      matches.push({ name: entry.name });
    }
  }

  await auditLog(c, "search", {
    path: resolved.normalized,
    username: session.user,
    query,
    matches: matches.length,
    storage: "s3",
  });

  return c.json({ matches });
}

async function handleS3Image(c: AppContext, session: SessionContext) {
  const requestPath = c.req.query("path");
  if (!requestPath) {
    return c.json({ error: "Path is required." }, 400);
  }

  let resolved;
  try {
    resolved = resolveSafeS3Path(requestPath, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  const info = await getS3PathInfo(session.rootReal, resolved.normalized);
  if (info.type === "none") {
    return c.json({ error: "Path not found." }, 404);
  }
  if (info.type !== "file") {
    return c.json({ error: "Path is not a file." }, 400);
  }
  if (!isImagePreviewable(resolved.normalized)) {
    return c.json({ error: "Image preview not available." }, 400);
  }

  const key = toS3Key(session.rootReal, resolved.normalized);
  const object = await s3GetObject(key);
  const ext = path.extname(resolved.normalized).toLowerCase();
  const mime = object.ContentType || IMAGE_MIME_BY_EXT[ext] || "application/octet-stream";
  const filename = path.posix.basename(resolved.normalized);
  c.header("Content-Type", mime);
  c.header("Content-Disposition", formatContentDisposition("inline", filename));

  await auditLog(c, "image_preview", { path: resolved.normalized, username: session.user, storage: "s3" });

  return c.body(object.Body as BodyInit);
}

async function handleS3EditOpen(c: AppContext, session: SessionContext) {
  const requestPath = c.req.query("path");
  if (!requestPath) {
    return c.json({ error: "Path is required." }, 400);
  }

  let resolved;
  try {
    resolved = resolveSafeS3Path(requestPath, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  const info = await getS3PathInfo(session.rootReal, resolved.normalized);
  if (info.type === "none") {
    return c.json({ error: "Path not found." }, 404);
  }
  if (info.type !== "file") {
    return c.json({ error: "Path is not a file." }, 400);
  }
  if (!isTextEditable(resolved.normalized)) {
    return c.json({ error: "Editor not available for this file type." }, 400);
  }
  if (info.size > MAX_EDIT_BYTES) {
    return c.json({ error: "File is too large to edit." }, 413);
  }

  const key = toS3Key(session.rootReal, resolved.normalized);
  const text = await s3ReadObjectText(key);

  await auditLog(c, "edit_open", { path: resolved.normalized, username: session.user, storage: "s3" });

  return c.json({
    name: path.posix.basename(resolved.normalized),
    size: info.size,
    mtime: info.mtime,
    path: resolved.normalized,
    content: text,
  });
}

async function handleS3Preview(c: AppContext, session: SessionContext) {
  const requestPath = c.req.query("path");
  if (!requestPath) {
    return c.json({ error: "Path is required." }, 400);
  }

  let resolved;
  try {
    resolved = resolveSafeS3Path(requestPath, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  const info = await getS3PathInfo(session.rootReal, resolved.normalized);
  if (info.type === "none") {
    return c.json({ error: "Path not found." }, 404);
  }
  if (info.type !== "file") {
    return c.json({ error: "Path is not a file." }, 400);
  }
  if (!isTextPreviewable(resolved.normalized)) {
    return c.json({ error: "Preview not available for this file type." }, 400);
  }
  if (info.size > MAX_PREVIEW_BYTES) {
    return c.json({ error: "File is too large to preview." }, 413);
  }

  const key = toS3Key(session.rootReal, resolved.normalized);
  const text = await s3ReadObjectText(key);

  await auditLog(c, "preview", { path: resolved.normalized, username: session.user, storage: "s3" });

  return c.json({
    name: path.posix.basename(resolved.normalized),
    size: info.size,
    mtime: info.mtime,
    content: text,
  });
}

async function handleS3EditSave(c: AppContext, session: SessionContext) {
  if (!canWrite(session.role)) {
    return c.json({ error: "Read-only account." }, 403);
  }

  const body = await readJsonBody<{ path?: string; content?: string }>(c);
  if (!body?.path) {
    return c.json({ error: "Path is required." }, 400);
  }
  if (typeof body.content !== "string") {
    return c.json({ error: "Content is required." }, 400);
  }

  let resolved;
  try {
    resolved = resolveSafeS3Path(body.path, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  const info = await getS3PathInfo(session.rootReal, resolved.normalized);
  if (info.type === "none") {
    return c.json({ error: "Path not found." }, 404);
  }
  if (info.type !== "file") {
    return c.json({ error: "Path is not a file." }, 400);
  }
  if (!isTextEditable(resolved.normalized)) {
    return c.json({ error: "Editor not available for this file type." }, 400);
  }

  const bytes = Buffer.byteLength(body.content, "utf8");
  if (bytes > MAX_EDIT_BYTES) {
    return c.json({ error: "File is too large to save." }, 413);
  }

  const key = toS3Key(session.rootReal, resolved.normalized);
  await s3PutObject(key, body.content, "text/plain; charset=utf-8");

  await auditLog(c, "edit_save", { path: resolved.normalized, username: session.user, bytes, storage: "s3" });

  return c.json({ ok: true });
}

async function handleS3Download(c: AppContext, session: SessionContext) {
  const requestPath = c.req.query("path");
  if (!requestPath) {
    return c.json({ error: "Path is required." }, 400);
  }

  let resolved;
  try {
    resolved = resolveSafeS3Path(requestPath, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  const info = await getS3PathInfo(session.rootReal, resolved.normalized);
  if (info.type === "none") {
    return c.json({ error: "Path not found." }, 404);
  }
  if (info.type !== "file") {
    return c.json({ error: "Path is not a file." }, 400);
  }

  const key = toS3Key(session.rootReal, resolved.normalized);
  const object = await s3GetObject(key);
  const filename = path.posix.basename(resolved.normalized);
  c.header("Content-Type", object.ContentType || "application/octet-stream");
  c.header("Content-Disposition", formatContentDisposition("attachment", filename));

  await auditLog(c, "download", { path: resolved.normalized, username: session.user, storage: "s3" });

  return c.body(object.Body as BodyInit);
}

async function handleS3Mkdir(c: AppContext, session: SessionContext) {
  if (!canWrite(session.role)) {
    return c.json({ error: "Read-only account." }, 403);
  }

  const body = await readJsonBody<{ path?: string; name?: string }>(c);
  if (!body) {
    return c.json({ error: "Invalid JSON." }, 400);
  }

  const parentPath = typeof body.path === "string" ? body.path : "/";
  const name = sanitizeName(body.name ?? "");
  if (!name) {
    return c.json({ error: "Folder name is required." }, 400);
  }

  let parent;
  try {
    parent = resolveSafeS3Path(parentPath, session.rootReal);
  } catch {
    return c.json({ error: "Parent path not found." }, 404);
  }

  const parentInfo = await getS3PathInfo(session.rootReal, parent.normalized);
  if (parentInfo.type === "none") {
    return c.json({ error: "Parent path not found." }, 404);
  }
  if (parentInfo.type !== "dir") {
    return c.json({ error: "Parent path is not a directory." }, 400);
  }

  const createdPath = parent.normalized === "/" ? `/${name}` : `${parent.normalized}/${name}`;
  const prefix = toS3Prefix(session.rootReal, createdPath);

  const fileKey = toS3Key(session.rootReal, createdPath);
  if ((await s3PrefixExists(prefix)) || (await s3ObjectExists(fileKey))) {
    return c.json({ error: "Folder already exists." }, 409);
  }

  await s3PutObject(prefix, "");
  await auditLog(c, "mkdir", { path: createdPath, username: session.user, storage: "s3" });
  return c.json({ ok: true });
}

async function handleS3Upload(c: AppContext, session: SessionContext) {
  if (!canWrite(session.role)) {
    return c.json({ error: "Read-only account." }, 403);
  }

  const form = await c.req.formData();
  const targetPath = form.get("path");
  if (typeof targetPath !== "string") {
    return c.json({ error: "Path is required." }, 400);
  }

  const overwrite = form.get("overwrite") === "1";

  let resolved;
  try {
    resolved = resolveSafeS3Path(targetPath, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  const dirInfo = await getS3PathInfo(session.rootReal, resolved.normalized);
  if (dirInfo.type === "none") {
    return c.json({ error: "Path not found." }, 404);
  }
  if (dirInfo.type !== "dir") {
    return c.json({ error: "Path is not a directory." }, 400);
  }

  const files = Array.from(form.entries())
    .filter(([key, value]) => key === "files" && value instanceof File)
    .map(([, value]) => value as File);

  if (files.length === 0) {
    return c.json({ error: "No files provided." }, 400);
  }

  const uploaded: string[] = [];
  for (const file of files) {
    const fileName = sanitizeName(file.name);
    if (!fileName) {
      return c.json({ error: "Invalid file name." }, 400);
    }

    const destPath = resolved.normalized === "/" ? `/${fileName}` : `${resolved.normalized}/${fileName}`;
    const key = toS3Key(session.rootReal, destPath);

    if (!overwrite) {
      const prefix = toS3Prefix(session.rootReal, destPath);
      if ((await s3ObjectExists(key)) || (await s3PrefixExists(prefix))) {
        return c.json({ error: `File exists: ${fileName}` }, 409);
      }
    }

    const buffer = new Uint8Array(await file.arrayBuffer());
    await s3PutObject(key, buffer, file.type || undefined);
    uploaded.push(fileName);
  }

  await auditLog(c, "upload", {
    path: resolved.normalized,
    files: uploaded,
    username: session.user,
    storage: "s3",
  });

  return c.json({ ok: true });
}

async function handleS3Move(c: AppContext, session: SessionContext) {
  if (!canWrite(session.role)) {
    return c.json({ error: "Read-only account." }, 403);
  }

  const body = await readJsonBody<{ from?: string; to?: string }>(c);
  if (!body) {
    return c.json({ error: "Invalid JSON." }, 400);
  }

  if (typeof body.from !== "string" || typeof body.to !== "string") {
    return c.json({ error: "From and to paths are required." }, 400);
  }

  let fromResolved;
  try {
    fromResolved = resolveSafeS3Path(body.from, session.rootReal);
  } catch {
    return c.json({ error: "Source path not found." }, 404);
  }

  if (fromResolved.normalized === "/") {
    return c.json({ error: "Cannot move the root." }, 400);
  }

  const dest = resolveDestinationPathS3(body.to);
  if (!dest) {
    return c.json({ error: "Destination path is invalid." }, 400);
  }

  if (dest.normalized === fromResolved.normalized) {
    return c.json({ error: "Destination matches source." }, 400);
  }

  const destParent = path.posix.dirname(dest.normalized);
  const destParentInfo = await getS3PathInfo(session.rootReal, destParent);
  if (destParentInfo.type !== "dir") {
    return c.json({ error: "Destination path is invalid." }, 400);
  }

  if (await s3PathExists(session.rootReal, dest.normalized)) {
    return c.json({ error: "Destination already exists." }, 409);
  }

  const info = await getS3PathInfo(session.rootReal, fromResolved.normalized);
  if (info.type === "none") {
    return c.json({ error: "Source path not found." }, 404);
  }

  if (info.type === "dir" && dest.normalized.startsWith(`${fromResolved.normalized}/`)) {
    return c.json({ error: "Cannot move a folder into itself." }, 400);
  }

  await moveS3Path(session.rootReal, fromResolved.normalized, dest.normalized, info.type);
  await auditLog(c, "move", {
    from: fromResolved.normalized,
    to: dest.normalized,
    username: session.user,
    storage: "s3",
  });
  return c.json({ ok: true });
}

async function handleS3Copy(c: AppContext, session: SessionContext) {
  if (!canWrite(session.role)) {
    return c.json({ error: "Read-only account." }, 403);
  }

  const body = await readJsonBody<{ from?: string; to?: string }>(c);
  if (!body) {
    return c.json({ error: "Invalid JSON." }, 400);
  }

  if (typeof body.from !== "string" || typeof body.to !== "string") {
    return c.json({ error: "From and to paths are required." }, 400);
  }

  let fromResolved;
  try {
    fromResolved = resolveSafeS3Path(body.from, session.rootReal);
  } catch {
    return c.json({ error: "Source path not found." }, 404);
  }

  const dest = resolveDestinationPathS3(body.to);
  if (!dest) {
    return c.json({ error: "Destination path is invalid." }, 400);
  }

  const destParent = path.posix.dirname(dest.normalized);
  const destParentInfo = await getS3PathInfo(session.rootReal, destParent);
  if (destParentInfo.type !== "dir") {
    return c.json({ error: "Destination path is invalid." }, 400);
  }

  if (await s3PathExists(session.rootReal, dest.normalized)) {
    return c.json({ error: "Destination already exists." }, 409);
  }

  const info = await getS3PathInfo(session.rootReal, fromResolved.normalized);
  if (info.type === "none") {
    return c.json({ error: "Source path not found." }, 404);
  }

  if (info.type === "dir" && dest.normalized.startsWith(`${fromResolved.normalized}/`)) {
    return c.json({ error: "Cannot copy a folder into itself." }, 400);
  }

  await copyS3Path(session.rootReal, fromResolved.normalized, dest.normalized, info.type);
  await auditLog(c, "copy", {
    from: fromResolved.normalized,
    to: dest.normalized,
    username: session.user,
    storage: "s3",
  });
  return c.json({ ok: true });
}

async function handleS3Trash(c: AppContext, session: SessionContext) {
  if (!canWrite(session.role)) {
    return c.json({ error: "Read-only account." }, 403);
  }

  const body = await readJsonBody<{ path?: string }>(c);
  if (!body) {
    return c.json({ error: "Invalid JSON." }, 400);
  }

  if (typeof body.path !== "string") {
    return c.json({ error: "Path is required." }, 400);
  }

  let resolved;
  try {
    resolved = resolveSafeS3Path(body.path, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  if (resolved.normalized === "/") {
    return c.json({ error: "Cannot delete the root." }, 400);
  }

  const info = await getS3PathInfo(session.rootReal, resolved.normalized);
  if (info.type === "none") {
    return c.json({ error: "Path not found." }, 404);
  }

  const { metaPrefix } = getTrashPrefixesS3(session.rootReal);
  const id = randomUUID();
  const fileName = sanitizeName(path.posix.basename(resolved.normalized)) ?? "item";
  const trashName = `${Date.now()}-${fileName}-${id}`;

  await moveS3Path(session.rootReal, resolved.normalized, `/.trash/${trashName}`, info.type);

  const record: TrashRecord = {
    id,
    name: fileName,
    originalPath: resolved.normalized,
    deletedAt: Date.now(),
    type: info.type === "dir" ? "dir" : "file",
    size: info.type === "file" ? info.size : 0,
    trashName,
  };

  await s3PutObject(`${metaPrefix}${id}.json`, JSON.stringify(record));
  await auditLog(c, "trash", { path: resolved.normalized, username: session.user, storage: "s3" });

  return c.json({ ok: true, item: record });
}

async function handleS3TrashList(c: AppContext, session: SessionContext) {
  const { metaPrefix } = getTrashPrefixesS3(session.rootReal);
  const records = await readS3TrashRecords(metaPrefix);
  await auditLog(c, "trash_list", { username: session.user, storage: "s3" });
  return c.json({ items: records, user: session.user, role: session.role });
}

async function handleS3TrashRestore(c: AppContext, session: SessionContext) {
  if (!canWrite(session.role)) {
    return c.json({ error: "Read-only account." }, 403);
  }

  const body = await readJsonBody<{ id?: string }>(c);
  if (!body) {
    return c.json({ error: "Invalid JSON." }, 400);
  }

  if (typeof body.id !== "string" || !body.id) {
    return c.json({ error: "Trash id is required." }, 400);
  }

  const { metaPrefix } = getTrashPrefixesS3(session.rootReal);
  const record = await readS3TrashRecord(metaPrefix, body.id);
  if (!record) {
    return c.json({ error: "Trash record not found." }, 404);
  }

  const normalized = normalizeRequestPath(record.originalPath);
  if (normalized === "/" || normalized.startsWith("/.trash")) {
    return c.json({ error: "Invalid restore path." }, 400);
  }

  const parentNormalized = path.posix.dirname(normalized);
  const parentInfo = await getS3PathInfo(session.rootReal, parentNormalized);
  if (parentInfo.type === "none") {
    return c.json({ error: "Restore location no longer exists." }, 409);
  }

  if (await s3PathExists(session.rootReal, normalized)) {
    return c.json({ error: "Restore target already exists." }, 409);
  }

  await moveS3Path(
    session.rootReal,
    `/.trash/${record.trashName}`,
    normalized,
    record.type
  );

  await s3DeleteObject(`${metaPrefix}${record.id}.json`);
  await auditLog(c, "restore", { path: normalized, username: session.user, storage: "s3" });

  return c.json({ ok: true });
}

async function handleS3Archive(c: AppContext, session: SessionContext) {
  const url = new URL(c.req.url);
  const requested = url.searchParams.getAll("path");
  if (requested.length === 0) {
    return c.json({ error: "No paths provided." }, 400);
  }

  const formatParam = url.searchParams.get("format");
  const formatRaw = formatParam ? formatParam.toLowerCase() : "zip";
  const format =
    formatRaw === "zip"
      ? "zip"
      : formatRaw === "targz" || formatRaw === "tar.gz" || formatRaw === "tgz"
        ? "targz"
        : null;
  if (!format) {
    return c.json({ error: "Invalid archive format." }, 400);
  }

  const items: Array<{ normalized: string; type: "file" | "dir" }> = [];
  const resolved: string[] = [];
  const isSingle = requested.length === 1;
  let singleName: string | null = null;

  for (const item of requested) {
    let resolvedItem;
    try {
      resolvedItem = resolveSafeS3Path(item, session.rootReal);
    } catch {
      return c.json({ error: "Path not found." }, 404);
    }

    if (resolvedItem.normalized === "/") {
      return c.json({ error: "Cannot archive the root." }, 400);
    }

    const info = await getS3PathInfo(session.rootReal, resolvedItem.normalized);
    if (info.type === "none") {
      return c.json({ error: "Path not found." }, 404);
    }

    items.push({ normalized: resolvedItem.normalized, type: info.type });
    const relative = resolvedItem.normalized.slice(1);
    resolved.push(relative);
    if (isSingle) {
      singleName = path.posix.basename(resolvedItem.normalized);
    }
  }

  if (resolved.length === 0) {
    return c.json({ error: "No valid paths provided." }, 400);
  }

  const totalBytes = format === "zip" ? await s3GetArchiveTotalBytes(session.rootReal, items) : 0;
  const useStore = format === "zip" && totalBytes >= ARCHIVE_LARGE_BYTES;
  const compression = format === "zip" ? (useStore ? "store" : "normal") : "gzip";

  const timestamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
  const baseName = singleName ?? `bundle-${timestamp}`;
  const safeBaseName = baseName.replace(/[\r\n"]/g, "") || "bundle";
  const archiveName = format === "zip" ? `${safeBaseName}.zip` : `${safeBaseName}.tar.gz`;

  const tempRoot = await fs.mkdtemp(path.join(os.tmpdir(), "bro-fm-s3-archive-"));
  try {
    for (const item of items) {
      if (item.type === "file") {
        const key = toS3Key(session.rootReal, item.normalized);
        const destPath = path.join(tempRoot, item.normalized.slice(1));
        await s3DownloadToFile(key, destPath);
        continue;
      }

      const dirPrefix = toS3Prefix(session.rootReal, item.normalized);
      const destRoot = path.join(tempRoot, item.normalized.slice(1));
      await fs.mkdir(destRoot, { recursive: true });
      const objects = await listS3Objects(dirPrefix);
      for (const object of objects) {
        if (!object.key) {
          continue;
        }
        const relative = object.key.slice(dirPrefix.length);
        if (!relative) {
          continue;
        }
        const targetPath = path.join(destRoot, relative);
        if (object.key.endsWith("/")) {
          await fs.mkdir(targetPath, { recursive: true });
          continue;
        }
        await s3DownloadToFile(object.key, targetPath);
      }
    }

    let process;
    try {
      const cmd =
        format === "zip"
          ? ["zip", "-q", "-r", "-y", ...(useStore ? ["-0"] : []), "-", ...resolved]
          : ["tar", "-czf", "-", ...resolved];
      process = Bun.spawn({
        cmd,
        cwd: tempRoot,
        stdout: "pipe",
        stderr: "pipe",
      });
    } catch {
      return c.json({ error: "Archive tool is not available." }, 500);
    }

    process.exited.then((code) => {
      if (code !== 0) {
        process.stderr
          ?.text()
          .then((text) => console.error("Archive failed:", text.trim()))
          .catch(() => {});
      }
    });

    c.header("Content-Type", format === "zip" ? "application/zip" : "application/gzip");
    c.header("Content-Disposition", formatContentDisposition("attachment", archiveName));
    await auditLog(c, "archive", {
      paths: requested,
      format,
      compression,
      username: session.user,
      storage: "s3",
    });
    return c.body(process.stdout);
  } finally {
    await fs.rm(tempRoot, { recursive: true, force: true });
  }
}

async function handleLocalUploadInit(
  c: AppContext,
  session: SessionContext,
  payload: {
    path: string;
    name: string;
    size: number;
    totalChunks: number;
    overwrite: boolean;
  }
) {
  let resolved;
  try {
    resolved = await resolveSafePath(payload.path, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  const dirStat = await fs.stat(resolved.fullPath);
  if (!dirStat.isDirectory()) {
    return c.json({ error: "Path is not a directory." }, 400);
  }

  const destPath = path.join(resolved.fullPath, payload.name);
  if (!isWithinRoot(destPath, session.rootReal)) {
    return c.json({ error: "Invalid file path." }, 400);
  }

  if (!payload.overwrite && (await pathExists(destPath))) {
    return c.json({ error: `File exists: ${payload.name}` }, 409);
  }

  const uploadId = randomUUID();
  const dir = path.join(UPLOAD_TMP_DIR, uploadId);
  await fs.mkdir(dir, { recursive: true });
  const meta = {
    uploadId,
    path: resolved.normalized,
    name: payload.name,
    size: payload.size,
    totalChunks: payload.totalChunks,
    overwrite: payload.overwrite,
  };
  await fs.writeFile(path.join(dir, "meta.json"), JSON.stringify(meta));

  await auditLog(c, "upload_init", {
    path: resolved.normalized,
    name: payload.name,
    username: session.user,
  });

  return c.json({ uploadId });
}

async function handleLocalUploadStatus(c: AppContext, session: SessionContext, uploadId: string) {
  const dir = path.join(UPLOAD_TMP_DIR, uploadId);
  let entries: string[];
  try {
    entries = await fs.readdir(dir);
  } catch {
    return c.json({ error: "Upload not found." }, 404);
  }
  const uploadedParts = entries
    .filter((name) => name.endsWith(".part"))
    .map((name) => Number.parseInt(name.replace(/\.part$/, ""), 10))
    .filter((value) => Number.isFinite(value));
  return c.json({ uploadedParts });
}

async function handleLocalUploadChunk(
  c: AppContext,
  session: SessionContext,
  payload: { uploadId: string; partNumber: number; totalChunks: number; chunk: File }
) {
  const dir = path.join(UPLOAD_TMP_DIR, payload.uploadId);
  try {
    await fs.stat(dir);
  } catch {
    return c.json({ error: "Upload not found." }, 404);
  }

  const chunkPath = path.join(dir, `${payload.partNumber}.part`);
  await Bun.write(chunkPath, payload.chunk);
  return c.json({ ok: true });
}

async function handleLocalUploadComplete(
  c: AppContext,
  session: SessionContext,
  uploadId: string,
  totalChunks: number
) {
  const dir = path.join(UPLOAD_TMP_DIR, uploadId);
  let metaRaw: string;
  try {
    metaRaw = await fs.readFile(path.join(dir, "meta.json"), "utf8");
  } catch {
    return c.json({ error: "Upload not found." }, 404);
  }

  const meta = JSON.parse(metaRaw) as {
    path: string;
    name: string;
    overwrite: boolean;
  };

  let resolved;
  try {
    resolved = await resolveSafePath(meta.path, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  const destPath = path.join(resolved.fullPath, meta.name);
  if (!isWithinRoot(destPath, session.rootReal)) {
    return c.json({ error: "Invalid file path." }, 400);
  }

  if (!meta.overwrite && (await pathExists(destPath))) {
    return c.json({ error: `File exists: ${meta.name}` }, 409);
  }

  const handle = await fs.open(destPath, "w");
  try {
    for (let part = 1; part <= totalChunks; part += 1) {
      const chunkPath = path.join(dir, `${part}.part`);
      const chunkData = await fs.readFile(chunkPath);
      await handle.write(chunkData);
    }
  } catch (error) {
    await handle.close();
    return c.json({ error: "Failed to assemble upload." }, 500);
  }
  await handle.close();

  await fs.rm(dir, { recursive: true, force: true });
  await auditLog(c, "upload_complete", {
    path: resolved.normalized,
    name: meta.name,
    username: session.user,
  });

  return c.json({ ok: true });
}

async function handleS3UploadInit(
  c: AppContext,
  session: SessionContext,
  payload: {
    path: string;
    name: string;
    size: number;
    totalChunks: number;
    overwrite: boolean;
  }
) {
  let resolved;
  try {
    resolved = resolveSafeS3Path(payload.path, session.rootReal);
  } catch {
    return c.json({ error: "Path not found." }, 404);
  }

  const dirInfo = await getS3PathInfo(session.rootReal, resolved.normalized);
  if (dirInfo.type === "none") {
    return c.json({ error: "Path not found." }, 404);
  }
  if (dirInfo.type !== "dir") {
    return c.json({ error: "Path is not a directory." }, 400);
  }

  const destPath =
    resolved.normalized === "/" ? `/${payload.name}` : `${resolved.normalized}/${payload.name}`;
  const key = toS3Key(session.rootReal, destPath);

  if (!payload.overwrite) {
    const prefix = toS3Prefix(session.rootReal, destPath);
    if ((await s3ObjectExists(key)) || (await s3PrefixExists(prefix))) {
      return c.json({ error: `File exists: ${payload.name}` }, 409);
    }
  }

  const client = getS3Client();
  const response = await client.send(
    new CreateMultipartUploadCommand({
      Bucket: S3_BUCKET,
      Key: key,
    })
  );

  if (!response.UploadId) {
    return c.json({ error: "Failed to start upload." }, 500);
  }

  await auditLog(c, "upload_init", {
    path: resolved.normalized,
    name: payload.name,
    username: session.user,
    storage: "s3",
  });

  return c.json({ uploadId: response.UploadId, key });
}

async function handleS3UploadStatus(
  c: AppContext,
  session: SessionContext,
  uploadId: string,
  key: string
) {
  const client = getS3Client();
  let response;
  try {
    response = await client.send(
      new ListPartsCommand({
        Bucket: S3_BUCKET,
        Key: key,
        UploadId: uploadId,
      })
    );
  } catch (error) {
    if (isS3NotFound(error)) {
      return c.json({ error: "Upload not found." }, 404);
    }
    throw error;
  }

  const uploadedParts = (response.Parts ?? [])
    .map((part) => part.PartNumber)
    .filter((part): part is number => Number.isFinite(part));

  return c.json({ uploadedParts });
}

async function handleS3UploadChunk(
  c: AppContext,
  session: SessionContext,
  payload: { uploadId: string; key: string; partNumber: number; chunk: File }
) {
  const client = getS3Client();
  const data = new Uint8Array(await payload.chunk.arrayBuffer());
  await client.send(
    new UploadPartCommand({
      Bucket: S3_BUCKET,
      Key: payload.key,
      UploadId: payload.uploadId,
      PartNumber: payload.partNumber,
      Body: data,
    })
  );
  return c.json({ ok: true });
}

async function handleS3UploadComplete(
  c: AppContext,
  session: SessionContext,
  uploadId: string,
  key: string,
  totalChunks: number
) {
  const client = getS3Client();
  const response = await client.send(
    new ListPartsCommand({
      Bucket: S3_BUCKET,
      Key: key,
      UploadId: uploadId,
    })
  );
  const parts = (response.Parts ?? [])
    .filter((part) => part.ETag && part.PartNumber)
    .map((part) => ({
      ETag: part.ETag,
      PartNumber: part.PartNumber as number,
    }))
    .sort((a, b) => a.PartNumber - b.PartNumber);

  if (parts.length < totalChunks) {
    return c.json({ error: "Upload incomplete." }, 409);
  }

  await client.send(
    new CompleteMultipartUploadCommand({
      Bucket: S3_BUCKET,
      Key: key,
      UploadId: uploadId,
      MultipartUpload: { Parts: parts },
    })
  );

  await auditLog(c, "upload_complete", {
    key,
    username: session.user,
    storage: "s3",
  });

  return c.json({ ok: true });
}

async function handleS3Storage(c: AppContext, session: SessionContext) {
  const stats = await getS3StorageStats(session.rootReal);
  return c.json(stats);
}

function resolveStorageMode(): StorageMode {
  const raw = (process.env.STORAGE_MODE ?? "").trim().toLowerCase();
  if (raw === "s3") {
    return "s3";
  }
  if (raw === "local") {
    return "local";
  }
  if ((process.env.S3_BUCKET ?? "").trim()) {
    return "s3";
  }
  return "local";
}

function normalizeS3Prefix(value: string) {
  let prefix = value.trim().replace(/\\/g, "/");
  if (!prefix) {
    return "";
  }
  prefix = prefix.replace(/^\/+/, "").replace(/\/+$/, "");
  return `${prefix}/`;
}

function joinS3Prefix(base: string, extra: string) {
  const normalizedBase = normalizeS3Prefix(base);
  const normalizedExtra = extra.trim().replace(/^\/+/, "").replace(/\/+$/, "");
  if (!normalizedExtra) {
    return normalizedBase;
  }
  return `${normalizedBase}${normalizedExtra}/`;
}

function resolveUserRootS3(rootPath: string) {
  const normalized = normalizeRequestPath(rootPath);
  if (normalized === "/.trash" || normalized.startsWith("/.trash/")) {
    throw new Error("User root cannot be .trash");
  }
  const suffix = normalized === "/" ? "" : normalized.slice(1);
  const rootPrefix = joinS3Prefix(S3_ROOT_PREFIX, suffix);
  return { rootPath: normalized, rootReal: rootPrefix };
}

function resolveSafeS3Path(requestPath: string, rootPrefix: string) {
  const normalized = normalizeRequestPath(requestPath);
  if (normalized === "/.trash" || normalized.startsWith("/.trash/")) {
    throw new Error("Path not allowed");
  }
  return { normalized, rootPrefix };
}

function toS3Key(rootPrefix: string, normalizedPath: string) {
  if (normalizedPath === "/") {
    return rootPrefix;
  }
  return `${rootPrefix}${normalizedPath.slice(1)}`;
}

function toS3Prefix(rootPrefix: string, normalizedPath: string) {
  if (normalizedPath === "/") {
    return rootPrefix;
  }
  const normalized = normalizedPath.replace(/\/+$/, "").slice(1);
  return `${rootPrefix}${normalized}/`;
}

function getTrashPrefixesS3(rootPrefix: string) {
  const trashPrefix = `${rootPrefix}.trash/`;
  const metaPrefix = `${trashPrefix}.meta/`;
  return { trashPrefix, metaPrefix };
}

function getS3Client() {
  if (!s3Client) {
    throw new Error("S3 client is not configured.");
  }
  return s3Client;
}

function isS3NotFound(error: unknown) {
  if (!error || typeof error !== "object") {
    return false;
  }
  const maybe = error as { name?: string; $metadata?: { httpStatusCode?: number } };
  return maybe.name === "NotFound" || maybe.$metadata?.httpStatusCode === 404;
}

async function s3HeadObject(key: string) {
  const client = getS3Client();
  try {
    return await client.send(
      new HeadObjectCommand({
        Bucket: S3_BUCKET,
        Key: key,
      })
    );
  } catch (error) {
    if (isS3NotFound(error)) {
      return null;
    }
    throw error;
  }
}

async function s3ObjectExists(key: string) {
  return (await s3HeadObject(key)) !== null;
}

async function s3PrefixExists(prefix: string) {
  if (!prefix) {
    return true;
  }
  const client = getS3Client();
  const response = await client.send(
    new ListObjectsV2Command({
      Bucket: S3_BUCKET,
      Prefix: prefix,
      MaxKeys: 1,
    })
  );
  return (response.Contents ?? []).length > 0;
}

async function getS3PathInfo(rootPrefix: string, normalizedPath: string) {
  if (normalizedPath === "/") {
    return { type: "dir" as const, size: 0, mtime: 0 };
  }

  const key = toS3Key(rootPrefix, normalizedPath);
  const head = await s3HeadObject(key);
  if (head) {
    return {
      type: "file" as const,
      size: head.ContentLength ?? 0,
      mtime: head.LastModified ? head.LastModified.getTime() : 0,
    };
  }

  const prefix = toS3Prefix(rootPrefix, normalizedPath);
  if (await s3PrefixExists(prefix)) {
    return { type: "dir" as const, size: 0, mtime: 0 };
  }

  return { type: "none" as const, size: 0, mtime: 0 };
}

async function s3PathExists(rootPrefix: string, normalizedPath: string) {
  const info = await getS3PathInfo(rootPrefix, normalizedPath);
  return info.type !== "none";
}

async function listS3Entries(prefix: string, normalizedPath: string) {
  const client = getS3Client();
  const entries: Array<{ name: string; type: "dir" | "file"; size: number; mtime: number }> = [];
  let token: string | undefined;

  do {
    const response = await client.send(
      new ListObjectsV2Command({
        Bucket: S3_BUCKET,
        Prefix: prefix,
        Delimiter: "/",
        ContinuationToken: token,
      })
    );

    for (const common of response.CommonPrefixes ?? []) {
      if (!common.Prefix) {
        continue;
      }
      const name = common.Prefix.slice(prefix.length).replace(/\/$/, "");
      if (!name) {
        continue;
      }
      if (normalizedPath === "/" && name === ".trash") {
        continue;
      }
      entries.push({ name, type: "dir", size: 0, mtime: 0 });
    }

    for (const item of response.Contents ?? []) {
      if (!item.Key) {
        continue;
      }
      if (item.Key === prefix) {
        continue;
      }
      const name = item.Key.slice(prefix.length);
      if (!name || name.endsWith("/")) {
        continue;
      }
      entries.push({
        name,
        type: "file",
        size: item.Size ?? 0,
        mtime: item.LastModified ? item.LastModified.getTime() : 0,
      });
    }

    token = response.IsTruncated ? response.NextContinuationToken : undefined;
  } while (token);

  entries.sort((a, b) => {
    if (a.type !== b.type) {
      return a.type === "dir" ? -1 : 1;
    }
    return a.name.localeCompare(b.name, undefined, { sensitivity: "base" });
  });

  return entries;
}

async function listS3Objects(prefix: string) {
  const client = getS3Client();
  const objects: Array<{ key: string; size: number }> = [];
  let token: string | undefined;

  do {
    const response = await client.send(
      new ListObjectsV2Command({
        Bucket: S3_BUCKET,
        Prefix: prefix,
        ContinuationToken: token,
      })
    );

    for (const item of response.Contents ?? []) {
      if (!item.Key) {
        continue;
      }
      objects.push({ key: item.Key, size: item.Size ?? 0 });
    }

    token = response.IsTruncated ? response.NextContinuationToken : undefined;
  } while (token);

  return objects;
}

async function s3GetObject(key: string) {
  const client = getS3Client();
  return client.send(
    new GetObjectCommand({
      Bucket: S3_BUCKET,
      Key: key,
    })
  );
}

async function s3ReadObjectText(key: string) {
  const object = await s3GetObject(key);
  return await readS3BodyText(object.Body);
}

async function readS3BodyText(body: unknown) {
  const bytes = await readS3Body(body);
  return new TextDecoder().decode(bytes);
}

async function readS3Body(body: unknown): Promise<Uint8Array> {
  if (!body) {
    return new Uint8Array();
  }
  if (body instanceof Uint8Array) {
    return body;
  }
  if (typeof body === "string") {
    return new TextEncoder().encode(body);
  }
  if (body instanceof ReadableStream) {
    const response = new Response(body);
    return new Uint8Array(await response.arrayBuffer());
  }
  const maybeBody = body as { transformToByteArray?: () => Promise<Uint8Array>; arrayBuffer?: () => Promise<ArrayBuffer> };
  if (typeof maybeBody.transformToByteArray === "function") {
    return await maybeBody.transformToByteArray();
  }
  if (typeof maybeBody.arrayBuffer === "function") {
    return new Uint8Array(await maybeBody.arrayBuffer());
  }
  if (Symbol.asyncIterator in Object(body)) {
    const chunks: Buffer[] = [];
    for await (const chunk of body as AsyncIterable<Uint8Array | string>) {
      chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : Buffer.from(chunk));
    }
    return Buffer.concat(chunks);
  }
  return new Uint8Array();
}

async function s3PutObject(key: string, body: BodyInit | Uint8Array, contentType?: string) {
  const client = getS3Client();
  await client.send(
    new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key: key,
      Body: body,
      ContentType: contentType,
    })
  );
}

function getS3CopySource(key: string) {
  return `${S3_BUCKET}/${encodeURIComponent(key)}`;
}

async function s3CopyObject(fromKey: string, toKey: string) {
  const client = getS3Client();
  await client.send(
    new CopyObjectCommand({
      Bucket: S3_BUCKET,
      CopySource: getS3CopySource(fromKey),
      Key: toKey,
    })
  );
}

async function s3DeleteObject(key: string) {
  const client = getS3Client();
  await client.send(
    new DeleteObjectCommand({
      Bucket: S3_BUCKET,
      Key: key,
    })
  );
}

async function copyS3Path(rootPrefix: string, fromPath: string, toPath: string, type: "file" | "dir") {
  if (type === "file") {
    const fromKey = toS3Key(rootPrefix, fromPath);
    const toKey = toS3Key(rootPrefix, toPath);
    await s3CopyObject(fromKey, toKey);
    return;
  }

  const fromPrefix = toS3Prefix(rootPrefix, fromPath);
  const toPrefix = toS3Prefix(rootPrefix, toPath);
  const objects = await listS3Objects(fromPrefix);
  if (objects.length === 0) {
    await s3PutObject(toPrefix, "");
    return;
  }
  for (const object of objects) {
    const relative = object.key.slice(fromPrefix.length);
    const targetKey = `${toPrefix}${relative}`;
    await s3CopyObject(object.key, targetKey);
  }
}

async function moveS3Path(rootPrefix: string, fromPath: string, toPath: string, type: "file" | "dir") {
  if (type === "file") {
    const fromKey = toS3Key(rootPrefix, fromPath);
    const toKey = toS3Key(rootPrefix, toPath);
    await s3CopyObject(fromKey, toKey);
    await s3DeleteObject(fromKey);
    return;
  }

  const fromPrefix = toS3Prefix(rootPrefix, fromPath);
  const toPrefix = toS3Prefix(rootPrefix, toPath);
  const objects = await listS3Objects(fromPrefix);
  if (objects.length === 0) {
    await s3PutObject(toPrefix, "");
    return;
  }
  for (const object of objects) {
    const relative = object.key.slice(fromPrefix.length);
    const targetKey = `${toPrefix}${relative}`;
    await s3CopyObject(object.key, targetKey);
  }
  for (const object of objects) {
    await s3DeleteObject(object.key);
  }
}

function resolveDestinationPathS3(target: string) {
  const normalized = normalizeRequestPath(target);
  if (normalized === "/" || normalized.startsWith("/.trash")) {
    return null;
  }
  const parentNormalized = path.posix.dirname(normalized);
  const name = sanitizeName(path.posix.basename(normalized));
  if (!name) {
    return null;
  }
  return { normalized };
}

async function readS3TrashRecord(metaPrefix: string, id: string): Promise<TrashRecord | null> {
  const key = `${metaPrefix}${id}.json`;
  try {
    const text = await s3ReadObjectText(key);
    const parsed = JSON.parse(text) as TrashRecord;
    if (!parsed?.id || !parsed?.trashName || !parsed?.originalPath) {
      return null;
    }
    return parsed;
  } catch (error) {
    if (isS3NotFound(error)) {
      return null;
    }
    return null;
  }
}

async function readS3TrashRecords(metaPrefix: string): Promise<TrashRecord[]> {
  const records: TrashRecord[] = [];
  const objects = await listS3Objects(metaPrefix);
  for (const object of objects) {
    if (!object.key.endsWith(".json")) {
      continue;
    }
    const id = object.key.slice(metaPrefix.length).replace(/\.json$/, "");
    const record = await readS3TrashRecord(metaPrefix, id);
    if (record) {
      records.push(record);
    }
  }
  records.sort((a, b) => b.deletedAt - a.deletedAt);
  return records;
}

async function s3GetArchiveTotalBytes(
  rootPrefix: string,
  items: Array<{ normalized: string; type: "file" | "dir" }>
) {
  let total = 0;
  for (const item of items) {
    if (total >= ARCHIVE_LARGE_BYTES) {
      break;
    }
    if (item.type === "file") {
      const info = await getS3PathInfo(rootPrefix, item.normalized);
      total += info.size;
      continue;
    }
    const prefix = toS3Prefix(rootPrefix, item.normalized);
    const objects = await listS3Objects(prefix);
    for (const object of objects) {
      if (object.key.endsWith("/")) {
        continue;
      }
      total += object.size;
      if (total >= ARCHIVE_LARGE_BYTES) {
        break;
      }
    }
  }
  return total;
}

async function getS3StorageStats(rootPrefix: string) {
  const trashPrefix = `${rootPrefix}.trash/`;
  const objects = await listS3Objects(rootPrefix);
  let totalBytes = 0;
  let totalFiles = 0;

  for (const object of objects) {
    if (!object.key) {
      continue;
    }
    if (object.key.endsWith("/")) {
      continue;
    }
    if (trashPrefix && object.key.startsWith(trashPrefix)) {
      continue;
    }
    totalBytes += object.size;
    totalFiles += 1;
  }

  return { totalBytes, totalFiles };
}

async function s3DownloadToFile(key: string, destPath: string) {
  await fs.mkdir(path.dirname(destPath), { recursive: true });
  const object = await s3GetObject(key);
  const body = object.Body as BodyInit;
  await Bun.write(destPath, body);
}
