import { createEffect, createMemo, createResource, createSignal, For, Show } from "solid-js";
import {
  clearRemoteCache,
  ensureKoreanMeanings,
  finishStudySession,
  generateJapaneseBoosterPack,
  generateSentenceLesson,
  getCourseMap,
  getDashboardSnapshot,
  getDueReviews,
  getLexemeDetail,
  getLlmSettings,
  getRemoteSyncStatus,
  getStudyStarts,
  loadServerConnectionConfig,
  prefetchCourseForOffline,
  recommendJapaneseBooster,
  saveServerConnectionConfig,
  saveLlmSettings,
  searchLexemes,
  syncPendingReviews,
  submitGeneratedLexemeFeedback,
  testApiServer,
  testLlmSettings,
  startStudySession,
  submitLexemeReview,
  type GeneratedSentenceLesson,
  type JapaneseBoosterRecommendation,
  type LlmProviderSettings,
  type RemoteSyncStatus,
  type ReviewQueueItem,
  type ServerConnectionConfig,
  type StudyStartOption,
} from "./lib/tauri";

type Page = "home" | "study";
type LessonMode = "word" | "quiz" | "sentence";
type HomeSection = "dashboard" | "courses" | "search" | "settings";
type BoosterProfileKey = "kindergarten" | "jlpt-n5" | "daily-conversation";
type BoosterThemeKey = "family" | "school" | "shopping" | "emotions";

const boosterProfiles: Array<{ key: BoosterProfileKey; label: string; description: string }> = [
  { key: "kindergarten", label: "유치원", description: "히라가나와 가장 쉬운 첫 단어" },
  { key: "jlpt-n5", label: "JLPT N5", description: "시험/교실/기초 문형 중심" },
  { key: "daily-conversation", label: "생활 회화", description: "일상 대화에서 바로 쓰는 단어" },
];

const boosterThemes: Array<{ key: BoosterThemeKey; label: string; description: string }> = [
  { key: "family", label: "가족/집", description: "가족, 집, 식사, 잠, 아침 준비처럼 매일 쓰는 단어" },
  { key: "school", label: "학교", description: "교실, 선생님, 공부, 시간표 같은 단어" },
  { key: "shopping", label: "쇼핑", description: "가게, 물건, 가격, 구매, 주문 관련 단어" },
  { key: "emotions", label: "감정", description: "좋다, 싫다, 피곤하다 같은 상태/감정 표현" },
];

declare global {
  interface Window {
    LinguaForgeAndroidTts?: {
      isAvailable?: () => boolean;
      speak?: (text: string, languageTag: string) => boolean;
    };
  }
}

function languageLabel(code: string) {
  if (code === "ja") return "일본어";
  if (code === "en") return "영어";
  return code.toUpperCase();
}

function gradeLabel(grade: string) {
  if (grade === "again") return "다시";
  if (grade === "hard") return "어려움";
  if (grade === "easy") return "쉬움";
  return "좋음";
}

function masteryLabel(level: string) {
  if (level === "new") return "새 카드";
  if (level === "learning") return "학습 중";
  if (level === "relearning") return "복습 필요";
  if (level === "mastered") return "안정권";
  return "복습 중";
}

function aiCourseBadge(courseKey: string) {
  if (courseKey === "ja-ai-kindergarten") return "AI 유치원";
  if (courseKey === "ja-ai-jlpt-n5") return "AI JLPT N5";
  if (courseKey === "ja-ai-daily-conversation") return "AI 회화";
  return null;
}

function inferBoosterProfileFromCourseKey(courseKey?: string | null): BoosterProfileKey | null {
  if (courseKey === "ja-ai-kindergarten") return "kindergarten";
  if (courseKey === "ja-ai-jlpt-n5") return "jlpt-n5";
  if (courseKey === "ja-ai-daily-conversation") return "daily-conversation";
  return null;
}

function inferBoosterThemeFromUnitTitle(title?: string | null): BoosterThemeKey | null {
  if (!title) return null;
  if (title.includes("가족/집")) return "family";
  if (title.includes("학교")) return "school";
  if (title.includes("쇼핑")) return "shopping";
  if (title.includes("감정")) return "emotions";
  return null;
}

function formatGlossText(value: string | null | undefined) {
  if (!value) return null;

  const cleaned = value
    .replace(/signficance/gi, "significance")
    .split(";")
    .map((part) => part.trim())
    .filter(Boolean);

  if (cleaned.length === 0) {
    return null;
  }

  const localized = cleaned.map((entry) => {
    const key = entry.toLowerCase();
    if (key === "meaning") return "뜻";
    if (key === "significance") return "의미";
    if (key === "sense") return "용법";
    return entry;
  });

  return localized.join(" · ");
}

function itemMeaning(item: Pick<ReviewQueueItem, "glossKo" | "glossEn">) {
  return formatGlossText(item.glossKo) ?? formatGlossText(item.glossEn) ?? "뜻 정보 없음";
}

function secondaryMeaning(item: Pick<ReviewQueueItem, "glossKo" | "glossEn">) {
  if (item.glossKo && item.glossEn) {
    return formatGlossText(item.glossEn);
  }
  return null;
}

function senseMeaning(sense: { glossKo: string | null; glossEn: string | null }) {
  return formatGlossText(sense.glossKo) ?? formatGlossText(sense.glossEn) ?? "뜻 정보 없음";
}

function waitForVoices() {
  if (typeof window === "undefined" || !("speechSynthesis" in window)) {
    return Promise.resolve([] as SpeechSynthesisVoice[]);
  }

  const current = window.speechSynthesis.getVoices();
  if (current.length > 0) {
    return Promise.resolve(current);
  }

  return new Promise<SpeechSynthesisVoice[]>((resolve) => {
    const timeout = window.setTimeout(() => resolve(window.speechSynthesis.getVoices()), 1200);
    window.speechSynthesis.onvoiceschanged = () => {
      window.clearTimeout(timeout);
      resolve(window.speechSynthesis.getVoices());
    };
  });
}

async function speakText(text: string, language: string) {
  if (typeof window === "undefined" || !text.trim()) {
    throw new Error("현재 환경에서는 발음을 재생할 수 없다.");
  }

  const languageTag = language === "ja" ? "ja-JP" : language === "en" ? "en-US" : "ko-KR";
  const androidTts = window.LinguaForgeAndroidTts;
  if (androidTts?.speak) {
    const ok = androidTts.speak(text, languageTag);
    if (ok) {
      return;
    }
  }

  if (!("speechSynthesis" in window)) {
    throw new Error("이 기기 WebView에서는 TTS를 바로 사용할 수 없다. 새 APK를 다시 설치해줘.");
  }

  const utterance = new SpeechSynthesisUtterance(text);
  utterance.lang = languageTag;
  utterance.rate = 0.92;
  utterance.pitch = 1;

  const voices = await waitForVoices();
  const voice = voices.find((candidate) => candidate.lang.startsWith(utterance.lang.slice(0, 2)));
  if (voice) {
    utterance.voice = voice;
  }

  await new Promise<void>((resolve, reject) => {
    utterance.onend = () => resolve();
    utterance.onerror = () => reject(new Error("발음 재생에 실패했다. 이 기기 TTS 설정을 확인해줘."));
    window.speechSynthesis.cancel();
    window.speechSynthesis.resume();
    window.speechSynthesis.speak(utterance);
    window.setTimeout(() => {
      if (!window.speechSynthesis.speaking && !window.speechSynthesis.pending) {
        reject(new Error("현재 기기에서 TTS 음성이 준비되지 않았다."));
      }
    }, 600);
  });
}

function App() {
  const [page, setPage] = createSignal<Page>("home");
  const [homeSection, setHomeSection] = createSignal<HomeSection>("dashboard");
  const [lessonMode, setLessonMode] = createSignal<LessonMode>("word");
  const [query, setQuery] = createSignal("");
  const [selectedId, setSelectedId] = createSignal<number | null>(null);
  const [previewCourseKey, setPreviewCourseKey] = createSignal<string | null>(null);
  const [refreshKey, setRefreshKey] = createSignal(0);
  const [revealed, setRevealed] = createSignal(false);
  const [busyGrade, setBusyGrade] = createSignal<string | null>(null);
  const [startingCourseKey, setStartingCourseKey] = createSignal<string | null>(null);
  const [selectedCourseKey, setSelectedCourseKey] = createSignal<string | null>(null);
  const [quizFeedback, setQuizFeedback] = createSignal<string | null>(null);
  const [selectedWordId, setSelectedWordId] = createSignal<number | null>(null);
  const [selectedMeaningId, setSelectedMeaningId] = createSignal<number | null>(null);
  const [matchingFeedback, setMatchingFeedback] = createSignal<string | null>(null);
  const [matchingBusy, setMatchingBusy] = createSignal(false);
  const [generatedSentence, setGeneratedSentence] = createSignal<GeneratedSentenceLesson | null>(null);
  const [generatingSentence, setGeneratingSentence] = createSignal(false);
  const [sentenceError, setSentenceError] = createSignal<string | null>(null);
  const [settingsStatus, setSettingsStatus] = createSignal<string | null>(null);
  const [serverStatus, setServerStatus] = createSignal<string | null>(null);
  const [studyStatus, setStudyStatus] = createSignal<string | null>(null);
  const [audioStatus, setAudioStatus] = createSignal<string | null>(null);
  const [llmStatus, setLlmStatus] = createSignal<string | null>(null);
  const [testingLlm, setTestingLlm] = createSignal(false);
  const [testingApiServer, setTestingApiServer] = createSignal(false);
  const [generatingJapanesePack, setGeneratingJapanesePack] = createSignal(false);
  const [boosterProfile, setBoosterProfile] = createSignal<BoosterProfileKey>("daily-conversation");
  const [boosterTheme, setBoosterTheme] = createSignal<BoosterThemeKey>("family");
  const [boosterRecommendation, setBoosterRecommendation] = createSignal<JapaneseBoosterRecommendation | null>(null);
  const [feedbackStatus, setFeedbackStatus] = createSignal<string | null>(null);
  const [submittingFeedback, setSubmittingFeedback] = createSignal<"good" | "bad" | null>(null);
  const [syncStatus, setSyncStatus] = createSignal<RemoteSyncStatus>(getRemoteSyncStatus());
  const [syncMessage, setSyncMessage] = createSignal<string | null>(null);
  const [syncingNow, setSyncingNow] = createSignal(false);
  const [offlinePacking, setOfflinePacking] = createSignal(false);
  const [savingSettings, setSavingSettings] = createSignal(false);
  const [serverForm, setServerForm] = createSignal<ServerConnectionConfig>(loadServerConnectionConfig());
  const [llmForm, setLlmForm] = createSignal<LlmProviderSettings>({
    enabled: false,
    provider: "ollama",
    baseUrl: "http://127.0.0.1:11434",
    model: "qwen2.5:3b-instruct",
    apiKey: null,
  });

  const [dashboard, { refetch: refetchDashboard }] = createResource(refreshKey, () =>
    getDashboardSnapshot(),
  );
  const [results] = createResource(query, (value) => searchLexemes(value));
  const [studyStarts, { refetch: refetchStudyStarts }] = createResource(() => getStudyStarts());
  const [llmSettings, { refetch: refetchLlmSettings }] = createResource(() => getLlmSettings());

  const activeCourseKey = createMemo(
    () => selectedCourseKey() ?? dashboard.latest?.activeSession?.courseKey ?? undefined,
  );
  const remoteLoadError = createMemo(
    () => studyStarts.error?.message ?? dashboard.error?.message ?? llmSettings.error?.message ?? null,
  );
  const dueReviewSource = createMemo(() => ({
    refresh: refreshKey(),
    courseKey: activeCourseKey(),
  }));

  const [dueReviews, { refetch: refetchDueReviews }] = createResource(dueReviewSource, (source) =>
    getDueReviews(source.courseKey, 8),
  );

  const [detail, { refetch: refetchDetail }] = createResource(selectedId, (value) =>
    value == null ? null : getLexemeDetail(value),
  );
  const [courseMap] = createResource(previewCourseKey, (courseKey) =>
    courseKey ? getCourseMap(courseKey) : null,
  );

  const currentCard = createMemo(() => dueReviews.latest?.[0] ?? null);
  const activeCourse = createMemo(() =>
    (studyStarts.latest ?? []).find((option) => option.courseKey === activeCourseKey()) ?? null,
  );
  const recommendedStarts = createMemo(() => studyStarts.latest ?? []);
  const existingSentenceExample = createMemo(() => detail.latest?.examples[0] ?? null);
  const homePreviewCard = createMemo(() => dueReviews.latest?.[0] ?? null);
  const coursePreviewUnits = createMemo(() => {
    const units = courseMap.latest?.units ?? [];
    const currentUnits = units.filter((unit) => unit.isCurrent).slice(0, 2);
    if (currentUnits.length > 0) return currentUnits;
    return units.slice(0, 3);
  });

  function refreshSyncStatus() {
    setSyncStatus(getRemoteSyncStatus());
  }

  function llmSettingsChanged() {
    const latest = llmSettings.latest;
    if (!latest) return true;
    return JSON.stringify(latest) !== JSON.stringify(llmForm());
  }

  async function persistLlmSettingsIfNeeded() {
    if (!llmSettingsChanged()) {
      return llmSettings.latest ?? llmForm();
    }

    const saved = await saveLlmSettings(llmForm());
    setLlmForm(saved);
    await refetchLlmSettings();
    return saved;
  }

  async function refreshCurrentKoreanMeaning() {
    const lexemeIds = [...new Set([currentCard()?.lexemeId, selectedId()].filter((value): value is number => value != null))];
    if (lexemeIds.length === 0) {
      return;
    }

    const hints = await ensureKoreanMeanings(lexemeIds.slice(0, 2));
    if (hints.length > 0) {
      await Promise.all([refetchDueReviews(), selectedId() ? refetchDetail() : Promise.resolve(null)]);
    }
  }

  async function refreshBoosterRecommendation() {
    try {
      const recommendation = await recommendJapaneseBooster();
      setBoosterRecommendation(recommendation);
    } catch {
      setBoosterRecommendation(null);
    }
  }

  const warmedMeaningIds = new Set<number>();
  let meaningWarmupBlockedKey: string | null = null;

  const quizOptions = createMemo(() => {
    const current = currentCard();
    const others = (dueReviews.latest ?? []).filter((item) => item.lexemeId !== current?.lexemeId);
    const candidateOptions = [current, ...others].filter(
      (item): item is ReviewQueueItem => Boolean(item && itemMeaning(item) !== "뜻 정보 없음"),
    );
    if (candidateOptions.length <= 1) {
      return candidateOptions;
    }
    return candidateOptions.slice(0, 4).sort((left, right) => left.lexemeId - right.lexemeId);
  });

  const matchingItems = createMemo(() => {
    return (dueReviews.latest ?? [])
      .filter((item) => itemMeaning(item) !== "뜻 정보 없음")
      .slice(0, 4);
  });

  const matchingMeanings = createMemo(() => {
    const items = matchingItems();
    if (items.length <= 1) {
      return items;
    }
    return items.slice(1).concat(items[0]);
  });

  createEffect(() => {
    const active = dashboard.latest?.activeSession?.courseKey;
    if (active) {
      setSelectedCourseKey(active);
    }
  });

  createEffect(() => {
    if (llmSettings.latest) {
      setLlmForm(llmSettings.latest);
    }
  });

  createEffect(() => {
    void refreshBoosterRecommendation();
  });

  createEffect(() => {
    llmForm().baseUrl;
    llmForm().model;
    llmForm().provider;
    meaningWarmupBlockedKey = null;
  });

  createEffect(() => {
    const active = activeCourseKey();
    if (active) {
      setPreviewCourseKey(active);
      return;
    }

    const starts = studyStarts.latest ?? [];
    if (!previewCourseKey() && starts.length > 0) {
      setPreviewCourseKey(starts[0].courseKey);
    }
  });

  createEffect(() => {
    const card = currentCard();
    if (card) {
      setSelectedId(card.lexemeId);
      setStudyStatus(null);
    } else {
      const items = results.latest ?? [];
      if (items.length > 0 && !items.some((item) => item.id === selectedId())) {
        setSelectedId(items[0].id);
      }
    }

    setRevealed(false);
    setQuizFeedback(null);
    setSelectedWordId(null);
    setSelectedMeaningId(null);
    setMatchingFeedback(null);
    setGeneratedSentence(null);
    setSentenceError(null);
  });

  createEffect(() => {
    const lexemeIds = (dueReviews.latest ?? []).slice(0, 3).map((item) => item.lexemeId);
    if (lexemeIds.length === 0) return;
    void Promise.all(lexemeIds.map((lexemeId) => getLexemeDetail(lexemeId)));
  });

  createEffect(() => {
    const current = currentCard();
    const selected = detail.latest;
    const isJapaneseStudy = activeCourse()?.language === "ja" || selected?.language === "ja";
    if (!llmForm().enabled || !isJapaneseStudy) {
      return;
    }

    const candidates = [...new Set([current?.lexemeId, selectedId()].filter((value): value is number => value != null))]
      .filter((lexemeId) => !warmedMeaningIds.has(lexemeId));
    if (candidates.length === 0) {
      return;
    }

    const settingsKey = `${llmForm().provider}|${llmForm().baseUrl}|${llmForm().model}`;
    if (meaningWarmupBlockedKey === settingsKey) {
      return;
    }

    void ensureKoreanMeanings(candidates.slice(0, 2))
      .then(async (hints) => {
        candidates.forEach((lexemeId) => warmedMeaningIds.add(lexemeId));
        if (hints.length > 0) {
          await Promise.all([refetchDueReviews(), selectedId() ? refetchDetail() : Promise.resolve(null)]);
          refreshSyncStatus();
        }
      })
      .catch((error) => {
        meaningWarmupBlockedKey = settingsKey;
        setLlmStatus(`한국어 뜻 자동 생성 실패: ${String(error)}`);
      });
  });

  async function refreshAll() {
    setRefreshKey((value) => value + 1);
    await Promise.all([refetchDashboard(), refetchDueReviews(), refetchStudyStarts()]);
    refreshSyncStatus();
    await refreshBoosterRecommendation();
  }

  function openHomeSection(section: HomeSection) {
    setPage("home");
    setHomeSection(section);
  }

  function openStudySection(mode: LessonMode) {
    setPage("study");
    setLessonMode(mode);
  }

  async function ensureSessionId() {
    const active = dashboard.latest?.activeSession;
    if (active) return active.id;
    const session = await startStudySession("review");
    await refreshAll();
    return session.sessionId;
  }

  async function handleStartGeneralSession() {
    setSelectedCourseKey(null);
    setStudyStatus("복습 큐를 새로 불러오는 중이다...");
    await startStudySession("review");
    openStudySection("word");
    await refreshAll();
    refreshSyncStatus();
  }

  async function handleStartCourse(option: StudyStartOption) {
    setPreviewCourseKey(option.courseKey);
    setStartingCourseKey(option.courseKey);
    setSelectedCourseKey(option.courseKey);
    setStudyStatus("코스의 첫 단어를 준비하는 중이다...");
    try {
      await startStudySession(`course:${option.courseKey}`, option.courseKey);
      openStudySection("word");
      const [queue] = await Promise.all([getDueReviews(option.courseKey, 8), refreshAll(), getCourseMap(option.courseKey)]);
      await refetchDueReviews();
      if (queue[0]) {
        setSelectedId(queue[0].lexemeId);
        setStudyStatus(null);
      } else {
        setStudyStatus("이 코스에서 아직 바로 낼 카드가 없다. 서버 캐시를 다시 확인해보자.");
      }
      refreshSyncStatus();
    } finally {
      setStartingCourseKey(null);
    }
  }

  async function handleFinishSession() {
    const active = dashboard.latest?.activeSession;
    if (!active) return;
    await finishStudySession(active.id);
    await refreshAll();
    setSelectedCourseKey(null);
    openHomeSection("dashboard");
    refreshSyncStatus();
  }

  async function handleReview(item: ReviewQueueItem, grade: "again" | "hard" | "good" | "easy") {
    const token = `${item.lexemeId}:${grade}`;
    setBusyGrade(token);
    try {
      const sessionId = await ensureSessionId();
      await submitLexemeReview(sessionId, item.lexemeId, grade, undefined, activeCourseKey());
      await refreshAll();
      refreshSyncStatus();
    } finally {
      setBusyGrade(null);
    }
  }

  async function handleQuizAnswer(answerLexemeId: number) {
    const item = currentCard();
    if (!item) return;
    const isCorrect = answerLexemeId === item.lexemeId;
    setBusyGrade(`quiz:${answerLexemeId}`);
    setQuizFeedback(isCorrect ? "정답이다. 다음 카드로 넘어간다." : "정답이 아니었다. 이 카드를 한 번 더 보자.");
    try {
      const sessionId = await ensureSessionId();
      await submitLexemeReview(
        sessionId,
        item.lexemeId,
        isCorrect ? "good" : "again",
        undefined,
        activeCourseKey(),
      );
      await refreshAll();
      refreshSyncStatus();
    } finally {
      setBusyGrade(null);
    }
  }

  async function evaluateMatch(wordId: number, meaningId: number) {
    if (matchingBusy()) return;
    if (wordId !== meaningId) {
      setMatchingFeedback("짝이 맞지 않는다. 다시 골라보자.");
      setSelectedWordId(null);
      setSelectedMeaningId(null);
      return;
    }

    const target = matchingItems().find((item) => item.lexemeId === wordId);
    if (!target) return;

    setMatchingBusy(true);
    setMatchingFeedback("정답이다. 같은 유닛의 다음 카드로 넘어간다.");
    try {
      const sessionId = await ensureSessionId();
      await submitLexemeReview(sessionId, target.lexemeId, "good", undefined, activeCourseKey());
      await refreshAll();
      refreshSyncStatus();
    } finally {
      setMatchingBusy(false);
      setSelectedWordId(null);
      setSelectedMeaningId(null);
    }
  }

  function handleWordPick(lexemeId: number) {
    const meaningId = selectedMeaningId();
    setSelectedWordId(lexemeId);
    if (meaningId != null) {
      void evaluateMatch(lexemeId, meaningId);
    }
  }

  function handleMeaningPick(lexemeId: number) {
    const wordId = selectedWordId();
    setSelectedMeaningId(lexemeId);
    if (wordId != null) {
      void evaluateMatch(wordId, lexemeId);
    }
  }

  async function handleSaveLlmSettings() {
    setSavingSettings(true);
    setSettingsStatus(null);
    try {
      const saved = await persistLlmSettingsIfNeeded();
      setSettingsStatus("로컬 LLM provider 설정을 저장했다.");
      if (saved.enabled) {
        await refreshCurrentKoreanMeaning();
      }
    } catch (error) {
      setSettingsStatus(String(error));
    } finally {
      setSavingSettings(false);
    }
  }

  async function handleTestLlm() {
    setTestingLlm(true);
    setLlmStatus(null);
    try {
      const api = await testApiServer();
      const result = await testLlmSettings(llmForm());
      const saved = await saveLlmSettings(llmForm());
      setLlmForm(saved);
      await refetchLlmSettings();
      meaningWarmupBlockedKey = null;
      setLlmStatus(`API 서버 ${api.baseUrl} 확인 완료. ${result.message} 검증된 설정도 저장했다.`);
      if (saved.enabled) {
        await refreshCurrentKoreanMeaning();
      }
    } catch (error) {
      setLlmStatus(String(error));
    } finally {
      setTestingLlm(false);
    }
  }

  async function handleTestApiServer() {
    setTestingApiServer(true);
    setServerStatus(null);
    try {
      const result = await testApiServer();
      setServerStatus(`학습 서버 ${result.baseUrl} 에 정상 연결됐다.`);
    } catch (error) {
      setServerStatus(String(error));
    } finally {
      setTestingApiServer(false);
    }
  }

  async function handleGenerateJapanesePack() {
    setGeneratingJapanesePack(true);
    setLlmStatus(null);
    try {
      const result = await generateJapaneseBoosterPack(boosterProfile(), boosterTheme(), 10);
      meaningWarmupBlockedKey = null;
      setPreviewCourseKey(result.courseKey);
      setSelectedCourseKey(result.courseKey);
      await refreshAll();
      setLlmStatus(
        `${result.profileLabel} · ${result.themeLabel} 보강 완료: 새 단어 ${result.insertedCount}개, 기존 단어 재사용 ${result.attachedExistingCount}개, 건너뜀 ${result.skippedCount}개. ${result.unitTitle} 유닛에 추가했다.`,
      );
      if (result.generatedLexemeIds[0]) {
        setSelectedId(result.generatedLexemeIds[0]);
      }
      openHomeSection("courses");
    } catch (error) {
      setLlmStatus(String(error));
    } finally {
      setGeneratingJapanesePack(false);
    }
  }

  async function handleGeneratedFeedback(rating: "good" | "bad") {
    const current = currentCard();
    const lexemeId = current?.lexemeId ?? selectedId();
    if (!lexemeId) return;

    const profileKey = inferBoosterProfileFromCourseKey(activeCourseKey());
    const themeKey = inferBoosterThemeFromUnitTitle(current?.unitTitle) ?? boosterTheme();

    setSubmittingFeedback(rating);
    setFeedbackStatus(null);
    try {
      const result = await submitGeneratedLexemeFeedback(lexemeId, rating, profileKey ?? undefined, themeKey ?? undefined);
      setFeedbackStatus(result.message);
    } catch (error) {
      setFeedbackStatus(String(error));
    } finally {
      setSubmittingFeedback(null);
    }
  }

  async function handleSaveServerConnection() {
    const saved = saveServerConnectionConfig(serverForm());
    setServerForm(saved);
    try {
      await refreshAll();
      await refetchLlmSettings();
      setServerStatus(
        saved.apiBaseUrl
          ? `학습 서버를 ${saved.apiBaseUrl} 로 저장했다. 이제 이 기기에서 현재 머신의 DB와 Ollama를 사용할 수 있다.`
          : "원격 학습 서버 연결을 해제했다. 이 기기의 로컬 Tauri backend를 사용한다.",
      );
      refreshSyncStatus();
    } catch (error) {
      setServerStatus(`연결 저장 후 데이터를 불러오지 못했다: ${String(error)}`);
    }
  }

  async function handleSyncNow() {
    setSyncingNow(true);
    setSyncMessage(null);
    try {
      const next = await syncPendingReviews();
      setSyncStatus(next);
      setSyncMessage(
        next.pendingReviews > 0
          ? `아직 ${next.pendingReviews}개 복습 기록이 서버 전송 대기 중이다.`
          : "모바일에 쌓인 복습 기록을 현재 머신과 동기화했다.",
      );
      await refreshAll();
    } catch (error) {
      setSyncMessage(`동기화 실패: ${String(error)}`);
    } finally {
      setSyncingNow(false);
    }
  }

  async function handleOfflinePack() {
    const courseKey = previewCourseKey() ?? activeCourseKey();
    if (!courseKey) {
      setSyncMessage("먼저 코스를 하나 선택해야 오프라인 저장을 만들 수 있다.");
      return;
    }

    setOfflinePacking(true);
    setSyncMessage(null);
    try {
      const packed = await prefetchCourseForOffline(courseKey, 24);
      refreshSyncStatus();
      setSyncMessage(`코스 오프라인 저장 완료: 유닛 ${packed.unitCount}개, 카드 ${packed.cardCount}개를 기기에 저장했다.`);
    } catch (error) {
      setSyncMessage(`오프라인 저장 실패: ${String(error)}`);
    } finally {
      setOfflinePacking(false);
    }
  }

  function handleClearOfflineCache() {
    clearRemoteCache();
    refreshSyncStatus();
    setSyncMessage("현재 서버 기준 모바일 캐시를 비웠다. 다시 접속하면 새로 받아온다.");
  }

  function updateLlmForm<K extends keyof LlmProviderSettings>(key: K, value: LlmProviderSettings[K]) {
    setLlmForm((current) => ({ ...current, [key]: value }));
  }

  function updateServerForm<K extends keyof ServerConnectionConfig>(
    key: K,
    value: ServerConnectionConfig[K],
  ) {
    setServerForm((current) => ({ ...current, [key]: value }));
  }

  async function handleGenerateSentence() {
    const item = currentCard();
    if (!item) return;

    setGeneratingSentence(true);
    setSentenceError(null);
    try {
      await persistLlmSettingsIfNeeded();
      const supportLexemeIds = (dueReviews.latest ?? [])
        .slice(1, 4)
        .map((candidate) => candidate.lexemeId);
      const lesson = await generateSentenceLesson(item.lexemeId, supportLexemeIds);
      setGeneratedSentence(lesson);
    } catch (error) {
      setSentenceError(String(error));
    } finally {
      setGeneratingSentence(false);
    }
  }

  async function handleSpeak(text: string, language: string) {
    setAudioStatus(null);
    try {
      await speakText(text, language);
    } catch (error) {
      setAudioStatus(String(error));
    }
  }

  const home = (
    <div class="page-stack">
      <div class="section-switcher panel">
        <button class={`mode-tab ${homeSection() === "dashboard" ? "active" : ""}`} onClick={() => setHomeSection("dashboard")}>
          오늘 학습
        </button>
        <button class={`mode-tab ${homeSection() === "courses" ? "active" : ""}`} onClick={() => setHomeSection("courses")}>
          코스 선택
        </button>
        <button class={`mode-tab ${homeSection() === "search" ? "active" : ""}`} onClick={() => setHomeSection("search")}>
          사전 검색
        </button>
        <button class={`mode-tab ${homeSection() === "settings" ? "active" : ""}`} onClick={() => setHomeSection("settings")}>
          연결/설정
        </button>
      </div>

      <Show when={homeSection() === "dashboard"}>
        <>
      <section class="hero-panel">
        <div>
          <p class="eyebrow">LinguaForge</p>
          <h1>한국어 중심으로 시작하고, 학습은 별도 페이지에서 편하게 이어지는 로컬 학습 앱</h1>
          <p class="subtitle">
            메인은 한국어로 안내하고, 학습은 `학습 페이지`에서 단어 학습, 단어 퀴즈, 문장 학습으로 나눠서 진행한다.
            문장 예문은 로컬 LLM으로 새로 만들 수 있고, 발음은 기기 TTS로 바로 들을 수 있다.
          </p>
        </div>

        <div class="hero-actions">
          <button class="action-button primary" onClick={() => openStudySection("word")}>
            단어 학습 시작
          </button>
          <button class="action-button" onClick={() => openHomeSection("courses")}>
            코스 선택으로 이동
          </button>
        </div>
      </section>

      <section class="stats-grid">
        <article class="stat-card emphasis">
          <span>진행 중 세션</span>
          <strong>{dashboard.latest?.activeSession ? "있음" : "없음"}</strong>
          <small>{dashboard.latest?.activeSession?.courseKey ?? "아직 시작 전"}</small>
        </article>
        <article class="stat-card">
          <span>현재 복습</span>
          <strong>{dashboard.latest?.dueReviews ?? 0}</strong>
          <small>바로 처리 가능한 복습</small>
        </article>
        <article class="stat-card">
          <span>새 카드</span>
          <strong>{dashboard.latest?.newItems ?? 0}</strong>
          <small>아직 익히지 않은 항목</small>
        </article>
        <article class="stat-card">
          <span>오늘 기록</span>
          <strong>{dashboard.latest?.reviewEventsToday ?? 0}</strong>
          <small>오늘 누적 복습</small>
        </article>
      </section>

      <section class="panel hero-panel compact-mobile-hero">
        <div>
          <p class="panel-kicker">오늘 바로 할 카드</p>
          <Show when={homePreviewCard()} fallback={<h2>코스를 고르면 첫 카드부터 바로 학습할 수 있다.</h2>}>
            {(item) => (
              <>
                <h2>{item().displayForm}</h2>
                <p class="section-copy">{itemMeaning(item())}</p>
                <Show when={item().reading}>
                  <p class="support-copy">읽기: {item().reading}</p>
                </Show>
              </>
            )}
          </Show>
        </div>

        <div class="hero-actions">
          <button class="action-button primary" onClick={() => openStudySection("word")}>
            지금 학습 시작
          </button>
          <Show when={homePreviewCard()}>
            {(item) => (
              <button class="tts-button" onClick={() => void handleSpeak(item().displayForm, activeCourse()?.language ?? "ja")}>
                발음 듣기
              </button>
            )}
          </Show>
        </div>
      </section>
        </>
      </Show>

      <Show when={homeSection() === "courses"}>
        <section class="home-grid">
        <section class="panel">
          <div class="panel-head compact">
            <div>
              <p class="panel-kicker">학습 시작</p>
              <h2>수준별 추천 코스</h2>
            </div>
          </div>
          <p class="section-copy">
            완전 처음이면 유치원 코스부터, 기초가 있으면 JLPT N5나 영어 A1부터 시작하면 된다.
          </p>
          <Show when={boosterRecommendation()}>
            {(recommendation) => (
              <article class="empty-state">
                <p class="panel-kicker">자동 추천</p>
                <strong>
                  {recommendation().profileLabel} · {recommendation().themeLabel}
                </strong>
                <p>{recommendation().reason}</p>
                <div class="inline-actions">
                  <button
                    class="action-button"
                    onClick={() => {
                      setBoosterProfile(recommendation().profileKey as BoosterProfileKey);
                      setBoosterTheme(recommendation().themeKey as BoosterThemeKey);
                    }}
                  >
                    추천 조합 적용
                  </button>
                </div>
              </article>
            )}
          </Show>
          <div class="page-stack compact-stack">
            <p class="support-copy">일본어 DB가 부족하면 아래 스타일을 골라 AI로 새 단어 유닛을 만든다.</p>
            <div class="mode-tabs booster-tabs">
              <For each={boosterProfiles}>
                {(profile) => (
                  <button
                    class={`mode-tab ${boosterProfile() === profile.key ? "active" : ""}`}
                    onClick={() => setBoosterProfile(profile.key)}
                  >
                    {profile.label}
                  </button>
                )}
              </For>
            </div>
            <p class="support-copy">
              {boosterProfiles.find((profile) => profile.key === boosterProfile())?.description}
            </p>
            <div class="mode-tabs booster-tabs wide-booster-tabs">
              <For each={boosterThemes}>
                {(theme) => (
                  <button
                    class={`mode-tab ${boosterTheme() === theme.key ? "active" : ""}`}
                    onClick={() => setBoosterTheme(theme.key)}
                  >
                    {theme.label}
                  </button>
                )}
              </For>
            </div>
            <p class="support-copy">{boosterThemes.find((theme) => theme.key === boosterTheme())?.description}</p>
          </div>
          <div class="inline-actions">
            <button class="action-button" disabled={generatingJapanesePack()} onClick={handleGenerateJapanesePack}>
              {generatingJapanesePack()
                ? "생성 중..."
                : `${boosterProfiles.find((profile) => profile.key === boosterProfile())?.label ?? "일본어"} AI 보강`}
            </button>
            <button class="action-button" onClick={() => openStudySection("word")}>
              진행 중 학습으로 이동
            </button>
          </div>

          <Show when={remoteLoadError()}>
            <div class="empty-state error">{remoteLoadError()}</div>
          </Show>

          <div class="start-list">
            <For each={recommendedStarts()}>
              {(option) => (
                <article class={`start-card ${previewCourseKey() === option.courseKey ? "selected" : ""}`}>
                  <div class="start-top">
                    <div>
                      <p class="start-language">{languageLabel(option.language)}</p>
                      <strong>{option.name}</strong>
                    </div>
                    <div class="badge-stack">
                      <span class="badge accent">{option.levelLabel}</span>
                      <Show when={aiCourseBadge(option.courseKey)}>
                        {(label) => <span class="badge muted">{label()}</span>}
                      </Show>
                    </div>
                  </div>
                  <p class="start-reason">{option.recommendedReason}</p>
                  <p class="start-meta">
                    {option.unitCount}개 유닛 · {option.itemCount}개 항목
                  </p>
                  <Show when={option.description}>
                    <p class="start-description">{option.description}</p>
                  </Show>
                  <Show when={previewCourseKey() === option.courseKey && coursePreviewUnits().length > 0}>
                    <div class="unit-preview-row">
                      <For each={coursePreviewUnits()}>
                        {(unit) => (
                          <span class={`unit-preview-chip ${unit.isCurrent ? "current" : ""}`}>
                            {unit.title}
                          </span>
                        )}
                      </For>
                    </div>
                  </Show>
                  <div class="start-actions">
                    <button class="action-button" onClick={() => setPreviewCourseKey(option.courseKey)}>
                      코스 보기
                    </button>
                    <button
                      class="action-button primary"
                      disabled={startingCourseKey() !== null}
                      onClick={() => handleStartCourse(option)}
                    >
                      {startingCourseKey() === option.courseKey ? "시작 중..." : "이 코스로 시작"}
                    </button>
                  </div>
                </article>
              )}
            </For>
          </div>
        </section>

        <section class="panel">
          <div class="panel-head compact">
            <div>
              <p class="panel-kicker">코스 맵</p>
              <h2>{courseMap.latest?.name ?? "선택한 코스"}</h2>
            </div>
            <Show when={previewCourseKey() ? aiCourseBadge(previewCourseKey()!) : null}>
              {(label) => <span class="status-pill">{label()}</span>}
            </Show>
          </div>
          <Show when={courseMap.latest?.levelLabel || courseMap.latest?.recommendedReason}>
            <div class="badge-row">
              <Show when={courseMap.latest?.levelLabel}>
                <span class="badge accent">{courseMap.latest?.levelLabel}</span>
              </Show>
              <Show when={courseMap.latest?.recommendedReason}>
                <span class="badge muted">{courseMap.latest?.recommendedReason}</span>
              </Show>
            </div>
          </Show>
          <Show
            when={courseMap.latest}
            fallback={<div class="empty-state">추천 코스를 누르면 유닛 맵을 볼 수 있다.</div>}
          >
            {(map) => (
              <div class="map-list">
                <For each={map().units}>
                  {(unit) => (
                    <article class={`map-unit ${unit.isCompleted ? "completed" : ""} ${unit.isCurrent ? "current" : ""} ${unit.isLocked ? "locked" : ""}`}>
                      <div class="map-unit-top">
                        <span class="map-dot">{unit.unitOrder}</span>
                        <div>
                          <strong>{unit.title}</strong>
                          <p>
                            {unit.learnedCount}/{unit.totalItems} 학습 · {unit.reviewedCount}회 복습
                          </p>
                        </div>
                      </div>
                    </article>
                  )}
                </For>
              </div>
            )}
          </Show>
        </section>
      </section>
      </Show>

      <Show when={homeSection() === "settings"}>
        <section class="home-grid">
        <section class="panel">
          <div class="panel-head compact">
            <div>
              <p class="panel-kicker">원격 연결</p>
              <h2>현재 머신 연결 주소</h2>
            </div>
          </div>

          <p class="section-copy">
            안드로이드 앱이나 다른 기기에서 현재 머신의 `content.db`, `progress.db`, Ollama를 쓰려면 여기에서 학습 서버 주소를 저장하면 된다.
          </p>
          <p class="support-copy">
            `http://mindok98.tplinkdns.com` 처럼 포트 없이 넣어도 자동으로 `:8787` 을 붙여 저장한다.
          </p>

          <div class="form-grid single-column">
            <label class="field">
              <span>학습 서버 주소</span>
              <input
                type="text"
                value={serverForm().apiBaseUrl}
                onInput={(event) => updateServerForm("apiBaseUrl", event.currentTarget.value)}
                placeholder="http://192.168.0.10:8787"
              />
            </label>
          </div>

          <div class="inline-actions">
            <button class="action-button primary" onClick={handleSaveServerConnection}>
              연결 주소 저장
            </button>
            <button class="action-button" disabled={testingApiServer()} onClick={handleTestApiServer}>
              {testingApiServer() ? "테스트 중..." : "학습 서버 테스트"}
            </button>
            <button class="action-button" onClick={() => setServerForm(loadServerConnectionConfig())}>
              저장값 다시 불러오기
            </button>
          </div>

          <Show when={serverStatus()}>
            <p class="feedback-text">{serverStatus()}</p>
          </Show>

          <Show when={remoteLoadError()}>
            <div class="empty-state error">현재 서버에서 코스/대시보드를 못 불러오고 있다: {remoteLoadError()}</div>
          </Show>
        </section>

        <section class="panel">
          <div class="panel-head compact">
            <div>
              <p class="panel-kicker">모바일 저장</p>
              <h2>오프라인 학습과 동기화</h2>
            </div>
            <span class={`status-pill ${syncStatus().pendingReviews === 0 ? "success" : ""}`}>
              {syncStatus().pendingReviews === 0 ? "동기화 양호" : `${syncStatus().pendingReviews}개 대기`}
            </span>
          </div>

          <div class="stats-grid sync-stats-grid">
            <article class="stat-card">
              <span>대기 복습</span>
              <strong>{syncStatus().pendingReviews}</strong>
              <small>서버로 아직 못 보낸 기록</small>
            </article>
            <article class="stat-card">
              <span>캐시 코스</span>
              <strong>{syncStatus().cachedCourses}</strong>
              <small>기기에 저장된 코스 맵</small>
            </article>
            <article class="stat-card">
              <span>캐시 단어</span>
              <strong>{syncStatus().cachedLexemes}</strong>
              <small>기기에 저장된 카드 상세</small>
            </article>
          </div>

          <p class="section-copy">
            모바일은 학습 큐와 단어 상세를 기기에 저장하고, 복습 결과는 먼저 로컬에 기록한 뒤 현재 머신과 다시 연결되면 순서대로 동기화한다.
          </p>
          <p class="support-copy">
            충돌 정책: 카드 진행은 모바일에서 즉시 반영하고, 서버와 다시 연결되면 FIFO 순서로 업로드한다. 서버가 최종 일정 계산을 맡고 모바일 캐시는 다시 그 결과를 받아 갱신한다.
          </p>
          <Show when={syncStatus().lastSyncAt}>
            <p class="support-copy">마지막 동기화: {syncStatus().lastSyncAt}</p>
          </Show>

          <div class="inline-actions">
            <button class="action-button primary" disabled={syncingNow()} onClick={handleSyncNow}>
              {syncingNow() ? "동기화 중..." : "지금 동기화"}
            </button>
            <button class="action-button" disabled={offlinePacking()} onClick={handleOfflinePack}>
              {offlinePacking() ? "저장 중..." : "선택 코스 오프라인 저장"}
            </button>
            <button class="action-button" onClick={handleClearOfflineCache}>
              모바일 캐시 비우기
            </button>
          </div>

          <Show when={syncMessage()}>
            <p class="feedback-text">{syncMessage()}</p>
          </Show>
        </section>

        <section class="panel">
          <div class="panel-head compact">
            <div>
              <p class="panel-kicker">로컬 LLM</p>
              <h2>모델 제공자 설정</h2>
            </div>
            <span class={`status-pill ${llmForm().enabled ? "success" : ""}`}>
              {llmForm().enabled ? "사용 중" : "꺼짐"}
            </span>
          </div>

          <p class="section-copy">
            문장 학습에서 새 예문을 만들 때 사용한다. 한국어 해석과 설명도 여기서 함께 생성한다.
          </p>
          <p class="support-copy">
            주의: 여기의 LLM 주소는 모바일이 아니라 현재 머신에서 직접 접속 가능한 주소여야 한다. 같은 머신 Ollama면 외부 DDNS 대신 `http://127.0.0.1:11434` 를 쓰는 편이 가장 안전하다. 포트 없이 `http://host` 만 넣으면 Ollama는 `:11434` 를 자동으로 붙인다.
          </p>

          <div class="form-grid">
            <label class="field checkbox-field">
              <span>로컬 LLM 사용</span>
              <input
                type="checkbox"
                checked={llmForm().enabled}
                onChange={(event) => updateLlmForm("enabled", event.currentTarget.checked)}
              />
            </label>

            <label class="field">
              <span>모델 제공자</span>
              <select
                value={llmForm().provider}
                onChange={(event) => updateLlmForm("provider", event.currentTarget.value)}
              >
                <option value="ollama">Ollama</option>
                <option value="openai-compatible">OpenAI 호환 서버</option>
              </select>
            </label>

            <label class="field">
              <span>서버 주소</span>
              <input
                type="text"
                value={llmForm().baseUrl}
                onInput={(event) => updateLlmForm("baseUrl", event.currentTarget.value)}
                placeholder="http://127.0.0.1:11434"
              />
            </label>

            <label class="field">
              <span>모델 이름</span>
              <input
                type="text"
                value={llmForm().model}
                onInput={(event) => updateLlmForm("model", event.currentTarget.value)}
                placeholder="qwen2.5:3b-instruct"
              />
            </label>

            <Show when={llmForm().provider === "openai-compatible"}>
              <label class="field">
                <span>API Key (선택)</span>
                <input
                  type="password"
                  value={llmForm().apiKey ?? ""}
                  onInput={(event) => updateLlmForm("apiKey", event.currentTarget.value || null)}
                  placeholder="비워도 되는 로컬 서버면 그대로 사용"
                />
              </label>
            </Show>
          </div>

          <div class="inline-actions">
            <button class="action-button primary" disabled={savingSettings()} onClick={handleSaveLlmSettings}>
              {savingSettings() ? "저장 중..." : "설정 저장"}
            </button>
            <button class="action-button" disabled={testingLlm()} onClick={handleTestLlm}>
              {testingLlm() ? "테스트 중..." : "LLM 연결 테스트"}
            </button>
            <button class="action-button" onClick={() => void refetchLlmSettings()}>
              다시 불러오기
            </button>
          </div>

          <Show when={settingsStatus()}>
            <p class="feedback-text">{settingsStatus()}</p>
          </Show>
          <Show when={llmStatus()}>
            <p class="feedback-text">{llmStatus()}</p>
          </Show>
        </section>
      </section>
      </Show>

      <Show when={homeSection() === "search"}>
      <section class="home-grid secondary">
        <section class="panel">
          <div class="panel-head compact">
            <div>
              <p class="panel-kicker">사전 찾기</p>
              <h2>단어를 먼저 둘러보기</h2>
            </div>
          </div>

          <input
            class="search-input"
            type="text"
            placeholder="영어, 일본어, 읽기, 뜻으로 검색"
            value={query()}
            onInput={(event) => setQuery(event.currentTarget.value)}
          />

          <div class="search-layout">
            <div class="search-list">
              <For each={results.latest ?? []}>
                {(item) => (
                  <button class={`result-card ${selectedId() === item.id ? "selected" : ""}`} onClick={() => setSelectedId(item.id)}>
                    <div class="result-head">
                      <div>
                        <p class="result-surface">{item.displayForm}</p>
                        <Show when={item.reading}>
                          <p class="result-reading">{item.reading}</p>
                        </Show>
                      </div>
                      <span class="badge muted">{item.partOfSpeech}</span>
                    </div>
                    <p>{itemMeaning(item)}</p>
                  </button>
                )}
              </For>
            </div>

            <div class="detail-box">
              <Show when={detail.latest} fallback={<div class="empty-state">검색 결과를 누르면 상세 정보가 열린다.</div>}>
                {(item) => (
                  <div class="detail-body compact-detail">
                    <div class="detail-hero">
                      <div>
                        <p class="detail-surface">{item().displayForm}</p>
                        <Show when={item().reading}>
                          <p class="detail-reading">{item().reading}</p>
                        </Show>
                      </div>
                      <button class="tts-button" onClick={() => void handleSpeak(item().displayForm, item().language)}>
                        발음 듣기
                      </button>
                    </div>
                    <section class="detail-section">
                      <h3>뜻</h3>
                      <Show when={item().generatedMeaningKo}>
                        <article class="lesson-answer">
                          <p>{item().generatedMeaningKo}</p>
                          <Show when={item().generatedExplanationKo}>
                            <p class="support-copy">{item().generatedExplanationKo}</p>
                          </Show>
                        </article>
                      </Show>
                      <div class="sense-list">
                        <For each={item().senses.slice(0, 3)}>
                          {(sense) => (
                            <article class="sense-card">
                              <span class="sense-order">{sense.senseOrder}</span>
                              <div>
                                <p>{senseMeaning(sense)}</p>
                                <Show when={sense.glossKo && sense.glossEn}>
                                  <p class="support-copy">영문 참고: {formatGlossText(sense.glossEn)}</p>
                                </Show>
                              </div>
                            </article>
                          )}
                        </For>
                      </div>
                    </section>
                  </div>
                )}
              </Show>
            </div>
          </div>
        </section>
      </section>
      </Show>
    </div>
  );

  const study = (
    <div class="page-stack">
      <section class="study-header panel">
        <div class="page-nav">
          <button class="action-button" onClick={() => openHomeSection("dashboard")}>
            홈으로
          </button>
          <div>
            <p class="panel-kicker">학습 페이지</p>
            <h2>{activeCourse()?.name ?? "진행 중인 코스 없음"}</h2>
            <p class="section-copy">
              {activeCourse()?.recommendedReason ?? "코스를 선택하면 현재 유닛 중심으로 학습을 이어갈 수 있다."}
            </p>
          </div>
        </div>

        <div class="hero-actions">
          <button class="action-button" onClick={handleStartGeneralSession}>
            복습 새로고침
          </button>
          <button class="action-button primary" disabled={!dashboard.latest?.activeSession} onClick={handleFinishSession}>
            세션 종료
          </button>
        </div>

        <Show when={studyStatus() || audioStatus()}>
          <div class="study-feedback-strip">
            <Show when={studyStatus()}>
              <p class="feedback-text">{studyStatus()}</p>
            </Show>
            <Show when={audioStatus()}>
              <p class="feedback-text">{audioStatus()}</p>
            </Show>
          </div>
        </Show>
      </section>

      <section class="study-grid">
        <div class="page-stack">
          <section class="panel">
            <div class="panel-head compact">
              <div>
                <p class="panel-kicker">학습 모드</p>
                <h2>한 번에 한 가지에 집중</h2>
              </div>
            </div>

            <div class="mode-tabs">
              <button class={`mode-tab ${lessonMode() === "word" ? "active" : ""}`} onClick={() => openStudySection("word")}>
                단어 학습
              </button>
              <button class={`mode-tab ${lessonMode() === "quiz" ? "active" : ""}`} onClick={() => openStudySection("quiz")}>
                단어 퀴즈
              </button>
              <button class={`mode-tab ${lessonMode() === "sentence" ? "active" : ""}`} onClick={() => openStudySection("sentence")}>
                문장 학습
              </button>
            </div>

            <Show
              when={currentCard()}
              fallback={
                <div class="empty-state">
                  <p>학습할 카드가 없다. 코스를 먼저 고르거나 일본어 AI 보강으로 새 단어를 채워보자.</p>
                  <Show when={llmForm().enabled}>
                    <button class="action-button" disabled={generatingJapanesePack()} onClick={handleGenerateJapanesePack}>
                      {generatingJapanesePack() ? "생성 중..." : "일본어 AI 단어 보강"}
                    </button>
                  </Show>
                </div>
              }
            >
              {(item) => (
                <div class="lesson-card">
                  <div class="lesson-meta-row">
                    <span class="badge accent">{item().unitTitle ?? "현재 유닛"}</span>
                    <span class="badge muted">{masteryLabel(item().masteryLevel)}</span>
                    <Show when={item().isNew}>
                      <span class="badge">처음 보는 카드</span>
                    </Show>
                  </div>

                  <div class="lesson-top">
                    <div>
                      <p class="lesson-surface">{item().displayForm}</p>
                      <Show when={item().reading}>
                        <p class="lesson-reading">{item().reading}</p>
                      </Show>
                    </div>
                    <button class="tts-button" onClick={() => void handleSpeak(item().displayForm, detail.latest?.language ?? activeCourse()?.language ?? "ja")}>
                      발음 듣기
                    </button>
                  </div>

                  <Show when={lessonMode() === "word"}>
                    <div>
                      <Show
                        when={revealed()}
                        fallback={
                          <button class="action-button primary full" onClick={() => setRevealed(true)}>
                            뜻 보기
                          </button>
                        }
                      >
                        <div class="lesson-answer">
                          <p>{itemMeaning(item())}</p>
                        </div>

                        <div class="review-actions wide">
                          <For each={["again", "hard", "good", "easy"] as const}>
                            {(grade) => (
                              <button
                                class={`review-button ${grade === "again" ? "again" : ""} ${grade === "good" ? "good" : ""} ${grade === "easy" ? "easy" : ""}`}
                                disabled={busyGrade() !== null}
                                onClick={() => handleReview(item(), grade)}
                              >
                                {gradeLabel(grade)}
                              </button>
                            )}
                          </For>
                        </div>
                      </Show>
                    </div>
                  </Show>

                  <Show when={lessonMode() === "quiz"}>
                    <div class="quiz-block">
                      <p class="quiz-prompt">이 단어의 뜻으로 가장 자연스러운 한국어 해석을 골라줘.</p>
                      <div class="quiz-options">
                        <For each={quizOptions()}>
                          {(option) => (
                            <button
                              class="quiz-option"
                              disabled={busyGrade() !== null}
                              onClick={() => handleQuizAnswer(option.lexemeId)}
                            >
                              {itemMeaning(option)}
                            </button>
                          )}
                        </For>
                      </div>
                      <Show when={quizFeedback()}>
                        <p class="feedback-text">{quizFeedback()}</p>
                      </Show>
                    </div>
                  </Show>

                  <Show when={lessonMode() === "sentence"}>
                    <div class="sentence-block">
                      <div class="sentence-section">
                        <div class="panel-head compact slim-head">
                          <div>
                            <p class="panel-kicker">문장 학습</p>
                            <h2>배운 단어로 문장 이해하기</h2>
                          </div>
                          <button class="action-button primary" disabled={generatingSentence()} onClick={handleGenerateSentence}>
                            {generatingSentence() ? "예문 생성 중..." : "로컬 LLM 예문 생성"}
                          </button>
                        </div>

                        <Show when={existingSentenceExample()}>
                          {(example) => (
                            <article class="sentence-card muted-card">
                              <div class="sentence-head">
                                <strong>사전 예문</strong>
                                <button class="tts-button" onClick={() => void handleSpeak(example().sentence, detail.latest?.language ?? activeCourse()?.language ?? "ja")}>
                                  문장 듣기
                                </button>
                              </div>
                              <p class="sentence-text">{example().sentence}</p>
                              <Show when={example().sentenceReading}>
                                <p class="sentence-reading">{example().sentenceReading}</p>
                              </Show>
                              <p class="support-copy">한국어 학습 포인트: {itemMeaning(item())}</p>
                            </article>
                          )}
                        </Show>

                        <Show when={generatedSentence()}>
                          {(lesson) => (
                            <article class="sentence-card">
                              <div class="sentence-head">
                                <strong>로컬 LLM 새 예문</strong>
                                <button class="tts-button" onClick={() => void handleSpeak(lesson().sentence, detail.latest?.language ?? activeCourse()?.language ?? "ja")}>
                                  문장 듣기
                                </button>
                              </div>
                              <p class="sentence-text">{lesson().sentence}</p>
                              <p>{lesson().translationKo}</p>
                              <p class="support-copy">설명: {lesson().explanationKo}</p>
                              <p class="support-copy">팁: {lesson().usageTipKo}</p>
                              <p class="support-copy">생성 모델: {lesson().providerLabel}</p>
                            </article>
                          )}
                        </Show>

                        <Show when={sentenceError()}>
                          <div class="empty-state error">{sentenceError()}</div>
                        </Show>
                      </div>
                    </div>
                  </Show>
                </div>
              )}
            </Show>
          </section>

          <Show when={lessonMode() === "quiz"}>
          <section class="panel">
            <div class="panel-head compact">
              <div>
                <p class="panel-kicker">단어 퀴즈</p>
                <h2>짝맞추기 연습</h2>
              </div>
            </div>

            <Show
              when={matchingItems().length >= 2}
              fallback={<div class="empty-state">같은 유닛 카드가 2장 이상일 때 짝맞추기 연습이 열린다.</div>}
            >
              <div class="matching-grid">
                <div class="match-column">
                  <For each={matchingItems()}>
                    {(item) => (
                      <button
                        class={`match-chip ${selectedWordId() === item.lexemeId ? "selected" : ""}`}
                        disabled={matchingBusy()}
                        onClick={() => handleWordPick(item.lexemeId)}
                      >
                        <strong>{item.displayForm}</strong>
                        <Show when={item.reading}>
                          <span>{item.reading}</span>
                        </Show>
                      </button>
                    )}
                  </For>
                </div>

                <div class="match-column">
                  <For each={matchingMeanings()}>
                    {(item) => (
                      <button
                        class={`match-chip meaning ${selectedMeaningId() === item.lexemeId ? "selected" : ""}`}
                        disabled={matchingBusy()}
                        onClick={() => handleMeaningPick(item.lexemeId)}
                      >
                        {itemMeaning(item)}
                      </button>
                    )}
                  </For>
                </div>
              </div>
            </Show>

            <Show when={matchingFeedback()}>
              <p class="feedback-text">{matchingFeedback()}</p>
            </Show>
          </section>
          </Show>
        </div>

        <div class="page-stack">
          <Show when={lessonMode() !== "sentence"}>
          <section class="panel">
            <div class="panel-head compact">
              <div>
                <p class="panel-kicker">코스 진행도</p>
                <h2>{courseMap.latest?.name ?? "현재 코스"}</h2>
              </div>
            </div>

            <Show when={courseMap.latest} fallback={<div class="empty-state">진행 중인 코스를 시작하면 유닛 맵이 열린다.</div>}>
              {(map) => (
                <div class="map-list">
                  <For each={map().units}>
                    {(unit) => (
                      <article class={`map-unit ${unit.isCompleted ? "completed" : ""} ${unit.isCurrent ? "current" : ""} ${unit.isLocked ? "locked" : ""}`}>
                        <div class="map-unit-top">
                          <span class="map-dot">{unit.unitOrder}</span>
                          <div>
                            <strong>{unit.title}</strong>
                            <p>
                              {unit.learnedCount}/{unit.totalItems} 학습 · {unit.reviewedCount}회 복습
                            </p>
                          </div>
                        </div>
                      </article>
                    )}
                  </For>
                </div>
              )}
            </Show>
          </section>
          </Show>

          <Show when={lessonMode() !== "sentence"}>
          <section class="panel">
            <div class="panel-head compact">
              <div>
                <p class="panel-kicker">현재 유닛</p>
                <h2>다음 카드 목록</h2>
              </div>
            </div>

            <Show when={(dueReviews.latest?.length ?? 0) > 0} fallback={<div class="empty-state">아직 학습할 카드가 없다.</div>}>
              <div class="queue-list">
                <For each={dueReviews.latest ?? []}>
                  {(item) => (
                    <article class="queue-card">
                      <div class="queue-head">
                        <div>
                          <button class="queue-link" onClick={() => setSelectedId(item.lexemeId)}>
                            {item.displayForm}
                          </button>
                          <Show when={item.reading}>
                            <p>{item.reading}</p>
                          </Show>
                        </div>
                        <span class={`badge ${item.isNew ? "accent" : "muted"}`}>{masteryLabel(item.masteryLevel)}</span>
                      </div>
                      <p class="queue-gloss">{itemMeaning(item)}</p>
                      <Show when={secondaryMeaning(item)}>
                        <p class="support-copy">영문 참고: {secondaryMeaning(item)}</p>
                      </Show>
                      <Show when={item.unitTitle}>
                        <p class="queue-unit">{item.unitTitle}</p>
                      </Show>
                    </article>
                  )}
                </For>
              </div>
            </Show>
          </section>
          </Show>

          <section class="panel">
            <div class="panel-head compact">
              <div>
                <p class="panel-kicker">현재 단어 설명</p>
                <h2>한국어 중심 풀이</h2>
              </div>
            </div>

            <Show when={detail.latest} fallback={<div class="empty-state">현재 카드를 고르면 여기에서 뜻과 예문을 볼 수 있다.</div>}>
              {(item) => (
                <div class="detail-body compact-detail">
                  <div class="detail-hero">
                    <div>
                      <p class="detail-surface">{item().displayForm}</p>
                      <Show when={item().reading}>
                        <p class="detail-reading">{item().reading}</p>
                      </Show>
                    </div>
                    <button class="tts-button" onClick={() => void handleSpeak(item().displayForm, item().language)}>
                      발음 듣기
                    </button>
                  </div>

                  <section class="detail-section">
                    <h3>뜻</h3>
                    <Show when={item().generatedMeaningKo}>
                      <article class="lesson-answer">
                        <p>{item().generatedMeaningKo}</p>
                        <Show when={item().generatedExplanationKo}>
                          <p class="support-copy">{item().generatedExplanationKo}</p>
                        </Show>
                        <Show when={item().generatedProviderLabel}>
                          <p class="support-copy">뜻 보강: {item().generatedProviderLabel}</p>
                        </Show>
                      </article>
                    </Show>
                    <Show when={inferBoosterProfileFromCourseKey(activeCourseKey())}>
                      <div class="inline-actions">
                        <button
                          class="action-button"
                          disabled={submittingFeedback() !== null}
                          onClick={() => void handleGeneratedFeedback("good")}
                        >
                          {submittingFeedback() === "good" ? "저장 중..." : "이 생성 좋음"}
                        </button>
                        <button
                          class="action-button"
                          disabled={submittingFeedback() !== null}
                          onClick={() => void handleGeneratedFeedback("bad")}
                        >
                          {submittingFeedback() === "bad" ? "저장 중..." : "이 생성 아쉬움"}
                        </button>
                      </div>
                    </Show>
                    <Show when={feedbackStatus()}>
                      <p class="feedback-text">{feedbackStatus()}</p>
                    </Show>
                    <div class="sense-list">
                      <For each={item().senses.slice(0, 4)}>
                        {(sense) => (
                          <article class="sense-card">
                            <span class="sense-order">{sense.senseOrder}</span>
                            <div>
                               <p>{senseMeaning(sense)}</p>
                               <Show when={sense.glossKo && sense.glossEn}>
                                 <p class="support-copy">영문 참고: {formatGlossText(sense.glossEn)}</p>
                               </Show>
                             </div>
                           </article>
                        )}
                      </For>
                    </div>
                  </section>

                  <Show when={item().kanji.length > 0}>
                    <section class="detail-section">
                      <h3>한자</h3>
                      <div class="kanji-grid">
                        <For each={item().kanji.slice(0, 4)}>
                          {(kanji) => (
                            <article class="kanji-card">
                              <div class="kanji-top">
                                <strong>{kanji.character}</strong>
                                <span>JLPT {kanji.jlptLevel ?? "-"}</span>
                              </div>
                              <p>{kanji.meanings.join(", ") || "뜻 정보 없음"}</p>
                            </article>
                          )}
                        </For>
                      </div>
                    </section>
                  </Show>
                </div>
              )}
            </Show>
          </section>
        </div>
      </section>
    </div>
  );

  return (
    <main class="shell">
      {page() === "home" ? home : study}
      <nav class="mobile-nav">
        <button class={`mobile-nav-button ${page() === "home" && homeSection() === "dashboard" ? "active" : ""}`} onClick={() => openHomeSection("dashboard")}>
          오늘
        </button>
        <button class={`mobile-nav-button ${page() === "home" && homeSection() === "courses" ? "active" : ""}`} onClick={() => openHomeSection("courses")}>
          코스
        </button>
        <button class={`mobile-nav-button ${page() === "study" ? "active" : ""}`} onClick={() => openStudySection("word")}>
          단어
        </button>
        <button class={`mobile-nav-button ${page() === "home" && homeSection() === "search" ? "active" : ""}`} onClick={() => openHomeSection("search")}>
          검색
        </button>
        <button class={`mobile-nav-button ${page() === "home" && homeSection() === "settings" ? "active" : ""}`} onClick={() => openHomeSection("settings")}>
          설정
        </button>
      </nav>
    </main>
  );
}

export default App;
