import { invoke } from "@tauri-apps/api/core";

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

function isTauriRuntime() {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
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
  if (!isTauriRuntime()) {
    return [] as SearchLexeme[];
  }

  return invoke<SearchLexeme[]>("search_lexemes", { query, limit });
}

export async function getLexemeDetail(lexemeId: number) {
  if (!isTauriRuntime()) {
    return null as LexemeDetail | null;
  }

  return invoke<LexemeDetail | null>("get_lexeme_detail", { lexemeId });
}

export async function getDashboardSnapshot() {
  if (!isTauriRuntime()) {
    return fallbackDashboard;
  }

  return invoke<DashboardSnapshot>("get_dashboard_snapshot");
}

export async function getStudyStarts() {
  if (!isTauriRuntime()) {
    return [] as StudyStartOption[];
  }

  return invoke<StudyStartOption[]>("get_study_starts");
}

export async function getCourseMap(courseKey: string) {
  if (!isTauriRuntime()) {
    return null as CourseMap | null;
  }

  return invoke<CourseMap>("get_course_map", { courseKey });
}

export async function startStudySession(mode = "review", courseKey?: string) {
  if (!isTauriRuntime()) {
    return {
      sessionId: 0,
      mode,
      startedAt: new Date().toISOString(),
      finishedAt: null,
      courseKey: courseKey ?? null,
    } as SessionState;
  }

  return invoke<SessionState>("start_study_session", { mode, courseKey });
}

export async function finishStudySession(sessionId: number) {
  if (!isTauriRuntime()) {
    return {
      sessionId,
      mode: "review",
      startedAt: new Date().toISOString(),
      finishedAt: new Date().toISOString(),
      courseKey: null,
    } as SessionState;
  }

  return invoke<SessionState>("finish_study_session", { sessionId });
}

export async function getDueReviews(courseKey?: string, limit = 12) {
  if (!isTauriRuntime()) {
    return [] as ReviewQueueItem[];
  }

  return invoke<ReviewQueueItem[]>("get_due_reviews", { courseKey, limit });
}

export async function submitLexemeReview(
  sessionId: number,
  lexemeId: number,
  grade: "again" | "hard" | "good" | "easy",
  responseTimeMs?: number,
) {
  if (!isTauriRuntime()) {
    throw new Error("Tauri runtime not available");
  }

  return invoke<ReviewQueueItem>("submit_lexeme_review", {
    sessionId,
    lexemeId,
    grade,
    responseTimeMs,
  });
}
