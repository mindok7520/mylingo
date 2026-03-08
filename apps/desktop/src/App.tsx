import { createEffect, createMemo, createResource, createSignal, For, Show } from "solid-js";
import {
  finishStudySession,
  getCourseMap,
  getDashboardSnapshot,
  getDueReviews,
  getLexemeDetail,
  getStudyStarts,
  searchLexemes,
  startStudySession,
  submitLexemeReview,
  type ReviewQueueItem,
  type StudyStartOption,
} from "./lib/tauri";

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

function itemMeaning(item: Pick<ReviewQueueItem, "glossKo" | "glossEn">) {
  return item.glossKo ?? item.glossEn ?? "뜻 정보 없음";
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function App() {
  const [query, setQuery] = createSignal("");
  const [selectedId, setSelectedId] = createSignal<number | null>(null);
  const [refreshKey, setRefreshKey] = createSignal(0);
  const [busyGrade, setBusyGrade] = createSignal<string | null>(null);
  const [startingCourseKey, setStartingCourseKey] = createSignal<string | null>(null);
  const [previewCourseKey, setPreviewCourseKey] = createSignal<string | null>(null);
  const [lessonMode, setLessonMode] = createSignal<"quiz" | "card">("quiz");
  const [revealed, setRevealed] = createSignal(false);
  const [lessonFeedback, setLessonFeedback] = createSignal<string | null>(null);
  const [selectedWordId, setSelectedWordId] = createSignal<number | null>(null);
  const [selectedMeaningId, setSelectedMeaningId] = createSignal<number | null>(null);
  const [matchingFeedback, setMatchingFeedback] = createSignal<string | null>(null);
  const [matchingBusy, setMatchingBusy] = createSignal(false);

  const [dashboard, { refetch: refetchDashboard }] = createResource(refreshKey, () =>
    getDashboardSnapshot(),
  );
  const [results] = createResource(query, (value) => searchLexemes(value));
  const [studyStarts, { refetch: refetchStudyStarts }] = createResource(() => getStudyStarts());

  const activeCourseKey = createMemo(() => dashboard.latest?.activeSession?.courseKey ?? undefined);
  const dueReviewSource = createMemo(() => ({
    refresh: refreshKey(),
    courseKey: activeCourseKey(),
  }));

  const [dueReviews, { refetch: refetchDueReviews }] = createResource(dueReviewSource, (source) =>
    getDueReviews(source.courseKey, 8),
  );

  const [detail] = createResource(selectedId, (value) =>
    value == null ? null : getLexemeDetail(value),
  );

  const [courseMap] = createResource(previewCourseKey, (courseKey) =>
    courseKey ? getCourseMap(courseKey) : null,
  );

  const currentCard = createMemo(() => dueReviews.latest?.[0] ?? null);
  const activeCourse = createMemo(() =>
    (studyStarts.latest ?? []).find((option) => option.courseKey === activeCourseKey()) ?? null,
  );
  const recommendedStarts = createMemo(() => (studyStarts.latest ?? []).slice(0, 6));

  const quizOptions = createMemo(() => {
    const items = dueReviews.latest ?? [];
    const options = items
      .filter((item) => itemMeaning(item) !== "뜻 정보 없음")
      .slice(0, 4)
      .map((item) => ({ lexemeId: item.lexemeId, label: itemMeaning(item) }));

    if (options.length <= 1) {
      return options;
    }

    return options.slice(1).concat(options[0]);
  });

  const matchingItems = createMemo(() => {
    const items = (dueReviews.latest ?? [])
      .filter((item) => itemMeaning(item) !== "뜻 정보 없음")
      .slice(0, 4);

    return items;
  });

  const matchingMeanings = createMemo(() => {
    const items = matchingItems();
    if (items.length <= 1) return items;
    return items.slice(1).concat(items[0]);
  });

  createEffect(() => {
    const active = activeCourseKey();
    const starts = studyStarts.latest ?? [];

    if (active) {
      setPreviewCourseKey(active);
      return;
    }

    if (!previewCourseKey() && starts.length > 0) {
      setPreviewCourseKey(starts[0].courseKey);
    }
  });

  createEffect(() => {
    const signature = (dueReviews.latest ?? []).map((item) => item.lexemeId).join(",");
    void signature;
    const card = currentCard();
    if (card) {
      setSelectedId(card.lexemeId);
    } else {
      const items = results.latest ?? [];
      if (items.length > 0 && !items.some((item) => item.id === selectedId())) {
        setSelectedId(items[0].id);
      }
    }
    setRevealed(false);
    setLessonFeedback(null);
    setSelectedWordId(null);
    setSelectedMeaningId(null);
    setMatchingFeedback(null);
  });

  async function refreshAll() {
    setRefreshKey((value) => value + 1);
    await Promise.all([refetchDashboard(), refetchDueReviews(), refetchStudyStarts()]);
  }

  async function ensureSessionId() {
    const active = dashboard.latest?.activeSession;
    if (active) return active.id;
    const session = await startStudySession("review");
    await refreshAll();
    return session.sessionId;
  }

  async function handleStartGeneralSession() {
    await startStudySession("review");
    await refreshAll();
  }

  async function handleStartCourse(option: StudyStartOption) {
    setPreviewCourseKey(option.courseKey);
    setStartingCourseKey(option.courseKey);
    try {
      await startStudySession(`course:${option.courseKey}`, option.courseKey);
      await refreshAll();
    } finally {
      setStartingCourseKey(null);
    }
  }

  async function handleFinishSession() {
    const active = dashboard.latest?.activeSession;
    if (!active) return;
    await finishStudySession(active.id);
    await refreshAll();
  }

  async function handleReview(item: ReviewQueueItem, grade: "again" | "hard" | "good" | "easy") {
    const token = `${item.lexemeId}:${grade}`;
    setBusyGrade(token);
    try {
      const sessionId = await ensureSessionId();
      await submitLexemeReview(sessionId, item.lexemeId, grade);
      await refreshAll();
    } finally {
      setBusyGrade(null);
    }
  }

  async function handleQuizAnswer(answerLexemeId: number) {
    const item = currentCard();
    if (!item) return;
    const correct = answerLexemeId === item.lexemeId;
    setBusyGrade(`quiz:${answerLexemeId}`);
    setLessonFeedback(correct ? "정답! 다음 카드로 넘어간다." : "아쉬워. 이 카드를 한 번 더 보자.");
    try {
      const sessionId = await ensureSessionId();
      await sleep(240);
      await submitLexemeReview(sessionId, item.lexemeId, correct ? "good" : "again");
      await refreshAll();
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

    const item = matchingItems().find((candidate) => candidate.lexemeId === wordId);
    if (!item) return;

    setMatchingBusy(true);
    setMatchingFeedback("정답! 이 카드를 통과했다.");
    try {
      const sessionId = await ensureSessionId();
      await sleep(180);
      await submitLexemeReview(sessionId, item.lexemeId, "good");
      await refreshAll();
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

  return (
    <main class="shell">
      <section class="hero-panel">
        <div>
          <p class="eyebrow">LinguaForge Desktop</p>
          <h1>유치원부터 코스 맵까지, 한 장씩 편하게 이어지는 학습 흐름</h1>
          <p class="subtitle">
            코스를 고르면 현재 유닛 카드가 바로 열리고, 객관식과 카드식 복습, 짝맞추기 연습,
            유닛 맵 확인까지 한 번에 이어서 할 수 있다.
          </p>
        </div>

        <div class="hero-actions">
          <button class="action-button primary" onClick={handleStartGeneralSession}>
            {dashboard.latest?.activeSession ? "세션 이어서 보기" : "바로 복습 시작"}
          </button>
          <button
            class="action-button"
            disabled={!dashboard.latest?.activeSession}
            onClick={handleFinishSession}
          >
            세션 종료
          </button>
        </div>
      </section>

      <section class="stats-grid">
        <article class="stat-card emphasis">
          <span>현재 카드</span>
          <strong>{currentCard() ? 1 : 0}</strong>
          <small>지금 바로 풀기</small>
        </article>
        <article class="stat-card">
          <span>유닛 대기</span>
          <strong>{dueReviews.latest?.length ?? 0}</strong>
          <small>현재 코스 기준</small>
        </article>
        <article class="stat-card">
          <span>전체 복습</span>
          <strong>{dashboard.latest?.dueReviews ?? 0}</strong>
          <small>모든 큐 합계</small>
        </article>
        <article class="stat-card">
          <span>새 항목</span>
          <strong>{dashboard.latest?.newItems ?? 0}</strong>
          <small>`progress.db` 기준</small>
        </article>
        <article class="stat-card">
          <span>오늘 기록</span>
          <strong>{dashboard.latest?.reviewEventsToday ?? 0}</strong>
          <small>오늘 처리한 복습</small>
        </article>
        <article class="stat-card">
          <span>코스 수</span>
          <strong>{(studyStarts.latest ?? []).length}</strong>
          <small>추천 시작점 포함</small>
        </article>
      </section>

      <section class="workspace-grid">
        <div class="column-stack">
          <section class="panel lesson-panel">
            <div class="panel-head compact">
              <div>
                <p class="panel-kicker">한 장씩 학습</p>
                <h2>지금 할 카드</h2>
              </div>
              <Show when={activeCourse()}>
                <span class="status-pill success">{activeCourse()?.name}</span>
              </Show>
            </div>

            <div class="mode-tabs">
              <button
                class={`mode-tab ${lessonMode() === "quiz" ? "active" : ""}`}
                onClick={() => setLessonMode("quiz")}
              >
                객관식
              </button>
              <button
                class={`mode-tab ${lessonMode() === "card" ? "active" : ""}`}
                onClick={() => setLessonMode("card")}
              >
                카드식
              </button>
            </div>

            <Show
              when={currentCard()}
              fallback={
                <div class="empty-state">
                  <Show
                    when={activeCourse()}
                    fallback={"아직 시작한 코스가 없다. 아래 추천 코스에서 첫 단계부터 바로 시작해보자."}
                  >
                    현재 코스에서 바로 낼 카드가 없다. 아래 코스 맵을 보고 다음 시작점을 골라볼 수 있다.
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
                      <span class="badge">새로 시작</span>
                    </Show>
                  </div>

                  <p class="lesson-surface">{item().displayForm}</p>
                  <Show when={item().reading}>
                    <p class="lesson-reading">{item().reading}</p>
                  </Show>

                  <Show
                    when={lessonMode() === "quiz" && quizOptions().length >= 2}
                    fallback={
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
                            <Show when={item().glossEn}>
                              <p class="gloss-en">{item().glossEn}</p>
                            </Show>
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
                    }
                  >
                    <div class="quiz-block">
                      <p class="quiz-prompt">이 단어의 뜻으로 가장 자연스러운 것을 골라줘.</p>
                      <div class="quiz-options">
                        <For each={quizOptions()}>
                          {(option) => (
                            <button
                              class="quiz-option"
                              disabled={busyGrade() !== null}
                              onClick={() => handleQuizAnswer(option.lexemeId)}
                            >
                              {option.label}
                            </button>
                          )}
                        </For>
                      </div>
                    </div>
                  </Show>

                  <Show when={lessonFeedback()}>
                    <p class="feedback-text">{lessonFeedback()}</p>
                  </Show>
                </div>
              )}
            </Show>
          </section>

          <section class="panel matching-panel">
            <div class="panel-head compact">
              <div>
                <p class="panel-kicker">짝맞추기</p>
                <h2>현재 유닛 빠른 연습</h2>
              </div>
            </div>

            <Show
              when={matchingItems().length >= 2}
              fallback={<div class="empty-state">짝맞추기는 같은 유닛 카드가 2장 이상 있을 때 열린다.</div>}
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

          <section class="panel start-panel">
            <div class="panel-head compact">
              <div>
                <p class="panel-kicker">학습 시작</p>
                <h2>수준별 추천 코스</h2>
              </div>
            </div>

            <p class="section-copy">
              완전 처음이면 히라가나/가타카나 유치원부터, 익숙하면 JLPT N5나 영어 A1부터 바로 시작하면 된다.
            </p>

            <div class="start-list">
              <For each={recommendedStarts()}>
                {(option) => (
                  <article
                    class={`start-card ${previewCourseKey() === option.courseKey ? "selected" : ""}`}
                  >
                    <div class="start-top">
                      <div>
                        <p class="start-language">{languageLabel(option.language)}</p>
                        <strong>{option.name}</strong>
                      </div>
                      <span class="badge accent">{option.levelLabel}</span>
                    </div>
                    <p class="start-reason">{option.recommendedReason}</p>
                    <p class="start-meta">
                      {option.unitCount}개 유닛 · {option.itemCount}개 항목
                    </p>
                    <Show when={option.description}>
                      <p class="start-description">{option.description}</p>
                    </Show>
                    <div class="start-actions">
                      <button class="action-button" onClick={() => setPreviewCourseKey(option.courseKey)}>
                        맵 보기
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

          <section class="panel queue-panel">
            <div class="panel-head compact">
              <div>
                <p class="panel-kicker">다음 카드</p>
                <h2>현재 유닛 대기열</h2>
              </div>
            </div>

            <Show
              when={(dueReviews.latest?.length ?? 0) > 0}
              fallback={<div class="empty-state">코스를 시작하면 여기에서 다음 카드 순서를 볼 수 있다.</div>}
            >
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
                        <span class={`badge ${item.isNew ? "accent" : "muted"}`}>
                          {masteryLabel(item.masteryLevel)}
                        </span>
                      </div>
                      <p class="queue-gloss">{itemMeaning(item)}</p>
                      <Show when={item.unitTitle}>
                        <p class="queue-unit">{item.unitTitle}</p>
                      </Show>
                    </article>
                  )}
                </For>
              </div>
            </Show>
          </section>

          <section class="panel search-panel">
            <div class="panel-head compact">
              <div>
                <p class="panel-kicker">검색</p>
                <h2>어휘 탐색</h2>
              </div>
              <Show when={results.loading}>
                <span class="status-pill">불러오는 중</span>
              </Show>
            </div>

            <input
              class="search-input"
              type="text"
              placeholder="영어, 일본어, 읽기, 뜻으로 검색"
              value={query()}
              onInput={(event) => setQuery(event.currentTarget.value)}
            />

            <div class="result-list">
              <Show when={results.error}>
                <div class="empty-state error">{String(results.error)}</div>
              </Show>

              <Show when={!results.error && (results.latest?.length ?? 0) === 0}>
                <div class="empty-state">
                  브라우저 미리보기에서는 로컬 DB 조회가 비어 보일 수 있다. Tauri 앱에서 실행하면 실제 결과를 볼 수 있다.
                </div>
              </Show>

              <For each={results.latest ?? []}>
                {(item) => (
                  <button
                    class={`result-card ${selectedId() === item.id ? "selected" : ""}`}
                    onClick={() => setSelectedId(item.id)}
                  >
                    <div class="result-head">
                      <div>
                        <p class="result-surface">{item.displayForm}</p>
                        <Show when={item.reading}>
                          <p class="result-reading">{item.reading}</p>
                        </Show>
                      </div>
                      <div class="badges">
                        <span class="badge muted">{item.language}</span>
                        <span class="badge">{item.partOfSpeech}</span>
                      </div>
                    </div>
                    <div class="result-body">
                      <Show when={item.glossKo}>
                        <p>{item.glossKo}</p>
                      </Show>
                      <Show when={item.glossEn}>
                        <p class="gloss-en">{item.glossEn}</p>
                      </Show>
                    </div>
                  </button>
                )}
              </For>
            </div>
          </section>
        </div>

        <div class="detail-stack">
          <section class="panel map-panel">
            <div class="panel-head compact">
              <div>
                <p class="panel-kicker">코스 맵</p>
                <h2>{courseMap.latest?.name ?? "선택한 코스"}</h2>
              </div>
              <Show when={activeCourse() && activeCourse()?.courseKey === previewCourseKey()}>
                <span class="status-pill success">현재 진행 중</span>
              </Show>
            </div>

            <Show
              when={courseMap.latest}
              fallback={<div class="empty-state">왼쪽 추천 코스를 누르면 유닛 맵을 볼 수 있다.</div>}
            >
              {(map) => (
                <div class="map-body">
                  <p class="section-copy">{map().recommendedReason}</p>
                  <div class="map-list">
                    <For each={map().units}>
                      {(unit) => (
                        <article
                          class={`map-unit ${unit.isCompleted ? "completed" : ""} ${unit.isCurrent ? "current" : ""} ${unit.isLocked ? "locked" : ""}`}
                        >
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
                </div>
              )}
            </Show>
          </section>

          <section class="panel detail-panel">
            <div class="panel-head">
              <div>
                <p class="panel-kicker">상세 보기</p>
                <h2>어휘 카드</h2>
              </div>
              <Show when={detail.loading}>
                <span class="status-pill">불러오는 중</span>
              </Show>
            </div>

            <Show
              when={detail.latest}
              fallback={<div class="empty-state">왼쪽 카드나 검색 결과를 누르면 상세 정보가 열린다.</div>}
            >
              {(item) => (
                <div class="detail-body">
                  <header class="detail-hero">
                    <div>
                      <p class="detail-surface">{item().displayForm}</p>
                      <Show when={item().reading}>
                        <p class="detail-reading">{item().reading}</p>
                      </Show>
                    </div>
                    <div class="badges align-end">
                      <span class="badge">{item().partOfSpeech}</span>
                      <Show when={item().jlptLevel}>
                        <span class="badge accent">JLPT N{item().jlptLevel}</span>
                      </Show>
                      <Show when={item().cefrLevel}>
                        <span class="badge accent">{item().cefrLevel}</span>
                      </Show>
                    </div>
                  </header>

                  <div class="meta-row">
                    <span>표제어 {item().lemma}</span>
                    <span>빈도 {item().frequencyRank ?? "-"}</span>
                    <span>품질 {item().qualityScore.toFixed(2)}</span>
                  </div>

                  <Show when={item().tags.length > 0}>
                    <div class="tag-row">
                      <For each={item().tags.slice(0, 8)}>{(tag) => <span class="badge muted">{tag}</span>}</For>
                    </div>
                  </Show>

                  <section class="detail-section">
                    <h3>뜻</h3>
                    <div class="sense-list">
                      <For each={item().senses}>
                        {(sense) => (
                          <article class="sense-card">
                            <span class="sense-order">{sense.senseOrder}</span>
                            <div>
                              <Show when={sense.glossKo}>
                                <p>{sense.glossKo}</p>
                              </Show>
                              <Show when={sense.glossEn}>
                                <p class="gloss-en">{sense.glossEn}</p>
                              </Show>
                              <Show when={sense.glossDetail}>
                                <p class="sense-detail">{sense.glossDetail}</p>
                              </Show>
                            </div>
                          </article>
                        )}
                      </For>
                    </div>
                  </section>

                  <section class="detail-section">
                    <h3>예문</h3>
                    <div class="example-list">
                      <For each={item().examples}>
                        {(example) => (
                          <article class="example-card">
                            <p>{example.sentence}</p>
                            <Show when={example.sentenceReading}>
                              <p class="example-reading">{example.sentenceReading}</p>
                            </Show>
                            <Show when={example.translationEn}>
                              <p class="gloss-en">{example.translationEn}</p>
                            </Show>
                          </article>
                        )}
                      </For>
                    </div>
                  </section>

                  <Show when={item().kanji.length > 0}>
                    <section class="detail-section">
                      <h3>한자</h3>
                      <div class="kanji-grid">
                        <For each={item().kanji}>
                          {(kanji) => (
                            <article class="kanji-card">
                              <div class="kanji-top">
                                <strong>{kanji.character}</strong>
                                <span>JLPT {kanji.jlptLevel ?? "-"}</span>
                              </div>
                              <p>{kanji.meanings.join(", ") || "뜻 정보 없음"}</p>
                              <p class="gloss-en">on {kanji.onyomi.join(", ") || "-"}</p>
                              <p class="gloss-en">kun {kanji.kunyomi.join(", ") || "-"}</p>
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
    </main>
  );
}

export default App;
