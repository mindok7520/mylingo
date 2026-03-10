import { invoke } from "@tauri-apps/api/core";

const API_BASE_URL_KEY = "linguaforge.apiBaseUrl";
const REMOTE_CACHE_VERSION = 2;

export type ServerConnectionConfig = {
  apiBaseUrl: string;
};

export type SearchLexeme = {
  id: number;
  language: string;
  displayForm: string;
  reading: string | null;
  partOfSpeech: string;
  glossEn: string | null;
  glossKo: string | null;
  frequencyRank: number | null;
  jlptLevel: number | null;
  cefrLevel: string | null;
};

export type LexemeSense = {
  senseOrder: number;
  glossEn: string | null;
  glossKo: string | null;
  glossDetail: string | null;
};

export type LexemeExample = {
  id: number;
  sentence: string;
  sentenceReading: string | null;
  translationEn: string | null;
  matchScore: number;
};

export type LexemeKanji = {
  character: string;
  grade: number | null;
  jlptLevel: number | null;
  frequencyRank: number | null;
  meanings: string[];
  onyomi: string[];
  kunyomi: string[];
};

export type LexemeDetail = {
  id: number;
  language: string;
  lemma: string;
  displayForm: string;
  reading: string | null;
  partOfSpeech: string;
  frequencyRank: number | null;
  jlptLevel: number | null;
  cefrLevel: string | null;
  qualityScore: number;
  generatedMeaningKo: string | null;
  generatedExplanationKo: string | null;
  generatedProviderLabel: string | null;
  senses: LexemeSense[];
  examples: LexemeExample[];
  kanji: LexemeKanji[];
  tags: string[];
};

export type ActiveSession = {
  id: number;
  mode: string;
  startedAt: string;
  courseKey: string | null;
};

export type DashboardSnapshot = {
  profileKey: string;
  dueReviews: number;
  newItems: number;
  totalReviewItems: number;
  reviewEventsToday: number;
  courseTemplates: number;
  lexemeCount: number;
  kanjiCount: number;
  activeSession: ActiveSession | null;
};

export type StudyStartOption = {
  courseKey: string;
  language: string;
  name: string;
  description: string | null;
  category: string;
  levelLabel: string;
  recommendedReason: string;
  unitCount: number;
  itemCount: number;
};

export type CourseMapUnit = {
  unitOrder: number;
  title: string;
  totalItems: number;
  learnedCount: number;
  reviewedCount: number;
  isCompleted: boolean;
  isCurrent: boolean;
  isLocked: boolean;
};

export type CourseMap = {
  courseKey: string;
  name: string;
  description: string | null;
  levelLabel: string;
  recommendedReason: string;
  units: CourseMapUnit[];
};

export type LlmProviderSettings = {
  enabled: boolean;
  provider: string;
  baseUrl: string;
  model: string;
  apiKey: string | null;
};

export type GeneratedSentenceLesson = {
  sentence: string;
  translationKo: string;
  explanationKo: string;
  usageTipKo: string;
  providerLabel: string;
};

export type ReviewQueueItem = {
  reviewItemId: number;
  lexemeId: number;
  displayForm: string;
  reading: string | null;
  glossEn: string | null;
  glossKo: string | null;
  partOfSpeech: string;
  scheduledAt: string | null;
  masteryLevel: string;
  intervalHours: number;
  dueState: string;
  unitOrder: number | null;
  unitTitle: string | null;
  isNew: boolean;
};

export type SessionState = {
  sessionId: number;
  mode: string;
  startedAt: string;
  finishedAt: string | null;
  courseKey: string | null;
};

export type RemoteSyncStatus = {
  pendingReviews: number;
  cachedCourses: number;
  cachedLexemes: number;
  lastSyncAt: string | null;
};

export type KoreanMeaningHint = {
  lexemeId: number;
  meaningKo: string;
  explanationKo: string | null;
  providerLabel: string;
};

export type LlmConnectionStatus = {
  providerLabel: string;
  baseUrl: string;
  modelFound: boolean;
  message: string;
};

export type JapaneseBoosterPack = {
  profileKey: string;
  profileLabel: string;
  themeKey: string;
  themeLabel: string;
  courseKey: string;
  unitTitle: string;
  insertedCount: number;
  attachedExistingCount: number;
  skippedCount: number;
  generatedLexemeIds: number[];
};

export type JapaneseBoosterRecommendation = {
  profileKey: string;
  profileLabel: string;
  themeKey: string;
  themeLabel: string;
  reason: string;
  currentCoverage: number;
  targetCoverage: number;
};

export type GeneratedLexemeFeedbackResult = {
  lexemeId: number;
  rating: string;
  profileKey: string | null;
  themeKey: string | null;
  message: string;
};

type RemoteCacheState = {
  version: number;
  dashboard?: DashboardSnapshot;
  studyStarts?: StudyStartOption[];
  llmSettings?: LlmProviderSettings;
  courseMaps: Record<string, CourseMap>;
  lexemeDetails: Record<string, LexemeDetail>;
  reviewQueues: Record<string, ReviewQueueItem[]>;
  hiddenReviewLexemeIds: Record<string, number[]>;
  pendingReviewSubmissions: PendingReviewSubmission[];
  lastSyncAt: string | null;
};

type PendingReviewSubmission = {
  sessionId: number;
  lexemeId: number;
  grade: "again" | "hard" | "good" | "easy";
  responseTimeMs?: number;
  courseKey?: string;
  queuedAt: string;
};

function isTauriRuntime() {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

function normalizeBaseUrl(value: string) {
  const trimmed = value.trim();
  if (!trimmed) return "";

  const withScheme = /^[a-z]+:\/\//i.test(trimmed) ? trimmed : `http://${trimmed}`;

  try {
    const parsed = new URL(withScheme);
    if (!parsed.port) {
      parsed.port = parsed.protocol === "https:" ? "443" : "8787";
    }
    parsed.pathname = "";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString().replace(/\/+$/, "");
  } catch {
    return withScheme.replace(/\/+$/, "");
  }
}

export function loadServerConnectionConfig(): ServerConnectionConfig {
  const fromEnv = typeof import.meta !== "undefined" ? import.meta.env?.VITE_LINGUAFORGE_API_BASE_URL : undefined;
  const fromStorage = typeof window !== "undefined" ? window.localStorage.getItem(API_BASE_URL_KEY) : null;
  return {
    apiBaseUrl: normalizeBaseUrl(fromStorage ?? fromEnv ?? ""),
  };
}

export function saveServerConnectionConfig(config: ServerConnectionConfig) {
  if (typeof window === "undefined") {
    return config;
  }

  const normalized = normalizeBaseUrl(config.apiBaseUrl);
  if (normalized) {
    window.localStorage.setItem(API_BASE_URL_KEY, normalized);
  } else {
    window.localStorage.removeItem(API_BASE_URL_KEY);
  }

  return { apiBaseUrl: normalized };
}

function activeApiBaseUrl() {
  return loadServerConnectionConfig().apiBaseUrl;
}

function remoteCacheStorageKey() {
  const baseUrl = activeApiBaseUrl();
  return baseUrl ? `linguaforge.remoteCache.${baseUrl}` : null;
}

function emptyRemoteCache(): RemoteCacheState {
  return {
    version: REMOTE_CACHE_VERSION,
    courseMaps: {},
    lexemeDetails: {},
    reviewQueues: {},
    hiddenReviewLexemeIds: {},
    pendingReviewSubmissions: [],
    lastSyncAt: null,
  };
}

function readRemoteCache() {
  if (typeof window === "undefined") {
    return emptyRemoteCache();
  }

  const key = remoteCacheStorageKey();
  if (!key) {
    return emptyRemoteCache();
  }

  try {
    const raw = window.localStorage.getItem(key);
    if (!raw) {
      return emptyRemoteCache();
    }
    const parsed = JSON.parse(raw) as Partial<RemoteCacheState>;
    if (parsed.version !== REMOTE_CACHE_VERSION) {
      return emptyRemoteCache();
    }
    return {
      ...emptyRemoteCache(),
      ...parsed,
      courseMaps: parsed.courseMaps ?? {},
      lexemeDetails: parsed.lexemeDetails ?? {},
      reviewQueues: parsed.reviewQueues ?? {},
      hiddenReviewLexemeIds: parsed.hiddenReviewLexemeIds ?? {},
      pendingReviewSubmissions: parsed.pendingReviewSubmissions ?? [],
      lastSyncAt: parsed.lastSyncAt ?? null,
    } satisfies RemoteCacheState;
  } catch {
    return emptyRemoteCache();
  }
}

function writeRemoteCache(cache: RemoteCacheState) {
  if (typeof window === "undefined") {
    return;
  }

  const key = remoteCacheStorageKey();
  if (!key) {
    return;
  }

  window.localStorage.setItem(key, JSON.stringify(cache));
}

function withRemoteCache(update: (cache: RemoteCacheState) => RemoteCacheState) {
  const next = update(readRemoteCache());
  writeRemoteCache(next);
  return next;
}

function reviewQueueCacheKey(courseKey?: string) {
  return courseKey ? `course:${courseKey}` : "global";
}

function dedupeReviewQueue(items: ReviewQueueItem[]) {
  const seen = new Set<number>();
  return items.filter((item) => {
    if (seen.has(item.lexemeId)) {
      return false;
    }
    seen.add(item.lexemeId);
    return true;
  });
}

function mergeReviewQueue(courseKey: string | undefined, incoming: ReviewQueueItem[]) {
  const key = reviewQueueCacheKey(courseKey);
  return withRemoteCache((cache) => {
    const hidden = new Set(cache.hiddenReviewLexemeIds[key] ?? []);
    const incomingIds = new Set(incoming.map((item) => item.lexemeId));
    const preserved = (cache.reviewQueues[key] ?? []).filter(
      (item) => !incomingIds.has(item.lexemeId) && !hidden.has(item.lexemeId),
    );

    cache.hiddenReviewLexemeIds[key] = [...hidden].filter((lexemeId) => !incomingIds.has(lexemeId));
    cache.reviewQueues[key] = dedupeReviewQueue([...incoming, ...preserved]);
    return cache;
  }).reviewQueues[key] ?? [];
}

function cachedReviewQueue(courseKey?: string) {
  return readRemoteCache().reviewQueues[reviewQueueCacheKey(courseKey)] ?? [];
}

function applyLocalReviewUpdate(
  courseKey: string | undefined,
  lexemeId: number,
  grade: "again" | "hard" | "good" | "easy",
  nextItem: ReviewQueueItem,
) {
  const key = reviewQueueCacheKey(courseKey);
  withRemoteCache((cache) => {
    const queue = [...(cache.reviewQueues[key] ?? [])];
    const index = queue.findIndex((item) => item.lexemeId === lexemeId);
    const hidden = new Set(cache.hiddenReviewLexemeIds[key] ?? []);

    if (index >= 0) {
      queue.splice(index, 1);
    }

    if (grade === "again") {
      hidden.delete(lexemeId);
      queue.push({ ...nextItem, dueState: "due", isNew: false });
    } else {
      hidden.add(lexemeId);
    }

    cache.reviewQueues[key] = dedupeReviewQueue(queue);
    cache.hiddenReviewLexemeIds[key] = [...hidden];
    return cache;
  });
}

function cacheOrThrow<T>(loadFromCache: () => T | null, error: unknown) {
  const cached = loadFromCache();
  if (cached != null) {
    return cached;
  }
  throw error;
}

function markCacheSynced() {
  withRemoteCache((cache) => {
    cache.lastSyncAt = new Date().toISOString();
    return cache;
  });
}

function enqueuePendingReview(submission: PendingReviewSubmission) {
  withRemoteCache((cache) => {
    cache.pendingReviewSubmissions = [...cache.pendingReviewSubmissions, submission];
    return cache;
  });
}

async function flushPendingReviewQueue() {
  if (!activeApiBaseUrl()) {
    return;
  }

  const cache = readRemoteCache();
  if (cache.pendingReviewSubmissions.length === 0) {
    return;
  }

  const remaining: PendingReviewSubmission[] = [];
  for (const submission of cache.pendingReviewSubmissions) {
    try {
      const nextItem = await httpJson<ReviewQueueItem>("/api/reviews/lexeme", {
        method: "POST",
        body: JSON.stringify({
          sessionId: submission.sessionId,
          lexemeId: submission.lexemeId,
          grade: submission.grade,
          responseTimeMs: submission.responseTimeMs,
        }),
      });
      applyLocalReviewUpdate(submission.courseKey, submission.lexemeId, submission.grade, nextItem);
      markCacheSynced();
    } catch {
      remaining.push(submission);
    }
  }

  withRemoteCache((nextCache) => {
    nextCache.pendingReviewSubmissions = remaining;
    return nextCache;
  });
}

function buildOfflineReviewResult(
  courseKey: string | undefined,
  lexemeId: number,
  grade: "again" | "hard" | "good" | "easy",
) {
  const queue = cachedReviewQueue(courseKey);
  const current = queue.find((item) => item.lexemeId === lexemeId) ?? queue[0];
  const now = new Date().toISOString();
  const fallback: ReviewQueueItem = {
    reviewItemId: current?.reviewItemId ?? 0,
    lexemeId,
    displayForm: current?.displayForm ?? "학습 카드",
    reading: current?.reading ?? null,
    glossEn: current?.glossEn ?? null,
    glossKo: current?.glossKo ?? null,
    partOfSpeech: current?.partOfSpeech ?? "",
    scheduledAt: grade === "again" ? now : null,
    masteryLevel: grade === "again" ? "relearning" : "learning",
    intervalHours: grade === "again" ? 0 : 8,
    dueState: grade === "again" ? "due" : "scheduled",
    unitOrder: current?.unitOrder ?? null,
    unitTitle: current?.unitTitle ?? null,
    isNew: false,
  };
  applyLocalReviewUpdate(courseKey, lexemeId, grade, fallback);
  return fallback;
}

export function getRemoteSyncStatus(): RemoteSyncStatus {
  const cache = readRemoteCache();
  return {
    pendingReviews: cache.pendingReviewSubmissions.length,
    cachedCourses: Object.keys(cache.courseMaps).length,
    cachedLexemes: Object.keys(cache.lexemeDetails).length,
    lastSyncAt: cache.lastSyncAt,
  };
}

export function clearRemoteCache() {
  const key = remoteCacheStorageKey();
  if (typeof window === "undefined" || !key) {
    return;
  }
  window.localStorage.removeItem(key);
}

export async function prefetchCourseForOffline(courseKey: string, queueLimit = 24) {
  const [courseMap, queue] = await Promise.all([getCourseMap(courseKey), getDueReviews(courseKey, queueLimit)]);
  await Promise.all(queue.map((item) => getLexemeDetail(item.lexemeId)));
  markCacheSynced();
  return {
    unitCount: courseMap?.units.length ?? 0,
    cardCount: queue.length,
  };
}

export async function syncPendingReviews() {
  await flushPendingReviewQueue();
  return getRemoteSyncStatus();
}

async function httpJson<T>(path: string, init?: RequestInit) {
  const baseUrl = activeApiBaseUrl();
  if (!baseUrl) {
    throw new Error("학습 서버 주소가 설정되지 않았다.");
  }

  let response: Response;
  try {
    response = await fetch(`${baseUrl}${path}`, {
      ...init,
      headers: {
        "Content-Type": "application/json",
        ...(init?.headers ?? {}),
      },
    });
  } catch (error) {
    throw new Error(`학습 서버 ${baseUrl} 에 연결하지 못했다. 서버 실행 상태와 주소를 다시 확인해줘. (${String(error)})`);
  }

  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`;
    try {
      const body = (await response.json()) as { error?: string };
      if (body.error) {
        message = body.error;
      }
    } catch {
      // ignore json parse error
    }
    throw new Error(message);
  }

  return (await response.json()) as T;
}

async function useHttpOrTauri<T>(
  tauriCommand: string,
  tauriPayload: Record<string, unknown>,
  httpPath: string,
  httpInit?: RequestInit,
) {
  if (activeApiBaseUrl()) {
    return httpJson<T>(httpPath, httpInit);
  }

  if (!isTauriRuntime()) {
    throw new Error("학습 서버 주소를 먼저 설정하거나 Tauri 앱에서 실행해줘.");
  }

  return invoke<T>(tauriCommand, tauriPayload);
}

export async function testApiServer() {
  if (!activeApiBaseUrl()) {
    throw new Error("학습 서버 주소를 먼저 저장해줘.");
  }

  const result = await httpJson<{ status: string }>("/health");
  return {
    ok: result.status === "ok",
    baseUrl: activeApiBaseUrl(),
  };
}

const fallbackDashboard: DashboardSnapshot = {
  profileKey: "default",
  dueReviews: 0,
  newItems: 0,
  totalReviewItems: 0,
  reviewEventsToday: 0,
  courseTemplates: 0,
  lexemeCount: 0,
  kanjiCount: 0,
  activeSession: null,
};

export async function searchLexemes(query: string, limit = 24) {
  if (!activeApiBaseUrl() && !isTauriRuntime()) {
    return [] as SearchLexeme[];
  }

  return useHttpOrTauri<SearchLexeme[]>(
    "search_lexemes",
    { query, limit },
    `/api/search?query=${encodeURIComponent(query)}&limit=${limit}`,
  );
}

export async function getLexemeDetail(lexemeId: number) {
  if (!activeApiBaseUrl() && !isTauriRuntime()) {
    return null as LexemeDetail | null;
  }

  try {
    const detail = await useHttpOrTauri<LexemeDetail | null>(
      "get_lexeme_detail",
      { lexemeId },
      `/api/lexemes/${lexemeId}`,
    );
    if (detail && activeApiBaseUrl()) {
      withRemoteCache((cache) => {
        cache.lexemeDetails[String(lexemeId)] = detail;
        return cache;
      });
      markCacheSynced();
    }
    return detail;
  } catch (error) {
    if (!activeApiBaseUrl()) {
      throw error;
    }
    return cacheOrThrow(() => readRemoteCache().lexemeDetails[String(lexemeId)] ?? null, error);
  }
}

export async function getDashboardSnapshot() {
  if (!activeApiBaseUrl() && !isTauriRuntime()) {
    return fallbackDashboard;
  }

  try {
    await flushPendingReviewQueue();
    const snapshot = await useHttpOrTauri<DashboardSnapshot>(
      "get_dashboard_snapshot",
      {},
      "/api/dashboard",
    );
    if (activeApiBaseUrl()) {
      withRemoteCache((cache) => {
        cache.dashboard = snapshot;
        return cache;
      });
      markCacheSynced();
    }
    return snapshot;
  } catch (error) {
    if (!activeApiBaseUrl()) {
      throw error;
    }
    return cacheOrThrow(() => readRemoteCache().dashboard ?? null, error);
  }
}

export async function getStudyStarts() {
  if (!activeApiBaseUrl() && !isTauriRuntime()) {
    return [] as StudyStartOption[];
  }

  try {
    const starts = await useHttpOrTauri<StudyStartOption[]>(
      "get_study_starts",
      {},
      "/api/study-starts",
    );
    if (activeApiBaseUrl()) {
      withRemoteCache((cache) => {
        cache.studyStarts = starts;
        return cache;
      });
      markCacheSynced();
    }
    return starts;
  } catch (error) {
    if (!activeApiBaseUrl()) {
      throw error;
    }
    return cacheOrThrow(() => readRemoteCache().studyStarts ?? null, error);
  }
}

export async function getCourseMap(courseKey: string) {
  if (!activeApiBaseUrl() && !isTauriRuntime()) {
    return null as CourseMap | null;
  }

  try {
    const courseMap = await useHttpOrTauri<CourseMap>(
      "get_course_map",
      { courseKey },
      `/api/course-map/${encodeURIComponent(courseKey)}`,
    );
    if (activeApiBaseUrl()) {
      withRemoteCache((cache) => {
        cache.courseMaps[courseKey] = courseMap;
        return cache;
      });
      markCacheSynced();
    }
    return courseMap;
  } catch (error) {
    if (!activeApiBaseUrl()) {
      throw error;
    }
    return cacheOrThrow(() => readRemoteCache().courseMaps[courseKey] ?? null, error);
  }
}

export async function getLlmSettings() {
  if (!activeApiBaseUrl() && !isTauriRuntime()) {
    return {
      enabled: false,
      provider: "ollama",
      baseUrl: "http://127.0.0.1:11434",
      model: "qwen2.5:3b-instruct",
      apiKey: null,
    } as LlmProviderSettings;
  }

  try {
    const settings = await useHttpOrTauri<LlmProviderSettings>(
      "get_llm_settings",
      {},
      "/api/llm-settings",
    );
    if (activeApiBaseUrl()) {
      withRemoteCache((cache) => {
        cache.llmSettings = settings;
        return cache;
      });
      markCacheSynced();
    }
    return settings;
  } catch (error) {
    if (!activeApiBaseUrl()) {
      throw error;
    }
    return cacheOrThrow(() => readRemoteCache().llmSettings ?? null, error);
  }
}

export async function saveLlmSettings(settings: LlmProviderSettings) {
  const saved = await useHttpOrTauri<LlmProviderSettings>(
    "save_llm_settings",
    { settings },
    "/api/llm-settings",
    {
      method: "PUT",
      body: JSON.stringify(settings),
    },
  );
  if (activeApiBaseUrl()) {
    withRemoteCache((cache) => {
      cache.llmSettings = saved;
      return cache;
    });
    markCacheSynced();
  }
  return saved;
}

export async function testLlmSettings(settings?: LlmProviderSettings) {
  return useHttpOrTauri<LlmConnectionStatus>(
    "test_llm_settings",
    { settings },
    "/api/llm-settings/test",
    {
      method: "POST",
      body: JSON.stringify({ settings }),
    },
  );
}

export async function generateSentenceLesson(
  lexemeId: number,
  supportLexemeIds?: number[],
) {
  return useHttpOrTauri<GeneratedSentenceLesson>(
    "generate_sentence_lesson",
    { lexemeId, supportLexemeIds },
    "/api/sentences/generate",
    {
      method: "POST",
      body: JSON.stringify({ lexemeId, supportLexemeIds }),
    },
  );
}

export async function ensureKoreanMeanings(lexemeIds: number[]) {
  return useHttpOrTauri<KoreanMeaningHint[]>(
    "ensure_korean_meanings",
    { lexemeIds },
    "/api/lexemes/korean-meanings",
    {
      method: "POST",
      body: JSON.stringify({ lexemeIds }),
    },
  );
}

export async function generateJapaneseBoosterPack(profileKey: string, themeKey: string, count = 8) {
  return useHttpOrTauri<JapaneseBoosterPack>(
    "generate_japanese_booster_pack",
    { profileKey, themeKey, count },
    "/api/lexemes/japanese-booster-pack",
    {
      method: "POST",
      body: JSON.stringify({ profileKey, themeKey, count }),
    },
  );
}

export async function recommendJapaneseBooster() {
  return useHttpOrTauri<JapaneseBoosterRecommendation>(
    "recommend_japanese_booster",
    {},
    "/api/lexemes/japanese-booster-recommendation",
  );
}

export async function submitGeneratedLexemeFeedback(
  lexemeId: number,
  rating: "good" | "bad",
  profileKey?: string,
  themeKey?: string,
) {
  return useHttpOrTauri<GeneratedLexemeFeedbackResult>(
    "submit_generated_lexeme_feedback",
    { lexemeId, profileKey, themeKey, rating },
    "/api/lexemes/generated-feedback",
    {
      method: "POST",
      body: JSON.stringify({ lexemeId, profileKey, themeKey, rating }),
    },
  );
}

export async function startStudySession(mode = "review", courseKey?: string) {
  if (!activeApiBaseUrl() && !isTauriRuntime()) {
    return {
      sessionId: 0,
      mode,
      startedAt: new Date().toISOString(),
      finishedAt: null,
      courseKey: courseKey ?? null,
    } as SessionState;
  }

  return useHttpOrTauri<SessionState>(
    "start_study_session",
    { mode, courseKey },
    "/api/session/start",
    {
      method: "POST",
      body: JSON.stringify({ mode, courseKey }),
    },
  );
}

export async function finishStudySession(sessionId: number) {
  if (!activeApiBaseUrl() && !isTauriRuntime()) {
    return {
      sessionId,
      mode: "review",
      startedAt: new Date().toISOString(),
      finishedAt: new Date().toISOString(),
      courseKey: null,
    } as SessionState;
  }

  return useHttpOrTauri<SessionState>(
    "finish_study_session",
    { sessionId },
    "/api/session/finish",
    {
      method: "POST",
      body: JSON.stringify({ sessionId }),
    },
  );
}

export async function getDueReviews(courseKey?: string, limit = 12) {
  if (!activeApiBaseUrl() && !isTauriRuntime()) {
    return [] as ReviewQueueItem[];
  }

  const query = new URLSearchParams();
  if (courseKey) query.set("courseKey", courseKey);
  query.set("limit", String(limit));

  try {
    await flushPendingReviewQueue();
    const reviews = await useHttpOrTauri<ReviewQueueItem[]>(
      "get_due_reviews",
      { courseKey, limit },
      `/api/reviews?${query.toString()}`,
    );
    if (activeApiBaseUrl()) {
      markCacheSynced();
      return mergeReviewQueue(courseKey, reviews).slice(0, limit);
    }
    return reviews;
  } catch (error) {
    if (!activeApiBaseUrl()) {
      throw error;
    }
    return cacheOrThrow(() => {
      const cached = cachedReviewQueue(courseKey).slice(0, limit);
      return cached.length > 0 ? cached : null;
    }, error);
  }
}

export async function submitLexemeReview(
  sessionId: number,
  lexemeId: number,
  grade: "again" | "hard" | "good" | "easy",
  responseTimeMs?: number,
  courseKey?: string,
) {
  try {
    const nextItem = await useHttpOrTauri<ReviewQueueItem>(
      "submit_lexeme_review",
      { sessionId, lexemeId, grade, responseTimeMs },
      "/api/reviews/lexeme",
      {
        method: "POST",
        body: JSON.stringify({ sessionId, lexemeId, grade, responseTimeMs }),
      },
    );
    if (activeApiBaseUrl()) {
      applyLocalReviewUpdate(courseKey, lexemeId, grade, nextItem);
      markCacheSynced();
    }
    return nextItem;
  } catch (error) {
    if (!activeApiBaseUrl()) {
      throw error;
    }
    enqueuePendingReview({ sessionId, lexemeId, grade, responseTimeMs, courseKey, queuedAt: new Date().toISOString() });
    return buildOfflineReviewResult(courseKey, lexemeId, grade);
  }
}
