# Execution Plan

## 현재 전략

기존 계획을 그대로 한 번에 구현하지 않고, 아래 순서로 리스크를 줄여가며 진행한다.

## Phase 0 - Foundation

- [x] 개선된 아키텍처와 DB 전략 확정
- [x] 데이터 소스 레지스트리 파일 정의
- [x] 자동 다운로드 CLI 초안 구현
- [x] 샘플 소스 다운로드 검증 (`google-10000`, `kanjidic2`, `jmdict`)

## Phase 1 - Data Platform

- [x] staging 디렉터리/메타데이터 포맷 확정
- [x] `content.db` / `progress.db` 마이그레이션 러너 구현
- [x] source import manifest / checksum / version lock 도입
- [x] JMdict, NGSL 파서 1차 구현
- [x] import 통계와 품질 리포트 출력

## Phase 2 - Language Content Build

- [x] JLPT / CEFR / frequency merge 규칙 1차 구현
- [x] example 품질 점수화 구현
- [x] course generator 1차 구현
- [ ] AI enrichment queue 설계

## Phase 3 - App Runtime

- [x] Tauri 앱 초기화
- [x] read-only content DB 연결
- [x] progress DB 연결
- [x] dashboard / course / search 기본 UI
- [x] lexeme detail / examples / kanji drill-down UI

## Phase 4 - Learning Engine

- [x] SRS review item 모델 1차 구현
- [x] flashcard / multiple choice / matching 1차 구현
- [x] 코스 맵 + 유닛 진행도 표시
- [x] 유치원 스타터 코스(히라가나/가타카나/영어) 구성
- [x] 홈/학습 페이지 분리와 한국어 중심 UI 구성
- [x] 단어 학습 / 단어 퀴즈 / 문장 학습 분리
- [x] 로컬 LLM provider 설정과 예문 생성 연결
- [x] TTS 기반 발음 재생 연결
- [ ] progress sync 정책 구현

## Phase 5 - Remote/Mobile Runtime

- [x] HTTP API 서버 초안 (`crates/linguaforge-api`)
- [x] Tauri command 재사용용 public API wrapper 정리
- [x] 프론트엔드 로컬 invoke / 원격 HTTP API 전환 레이어 구현
- [x] Android init / debug build 검증
- [ ] 원격 연결 상태 점검 UX 추가
- [ ] API 인증 / 토큰 정책 추가

## 이번 작업에서 실제로 진행한 것

1. DB를 content/progress 분리형으로 재설계했다.
2. 자동 데이터 수집용 Rust CLI를 만들었다.
3. 소스별 다운로드/압축 해제 설정을 `config/sources.toml` 로 외부화했다.
4. `google-10000`, `NGSL`, `JMdict` staged parser를 구현했다.
5. SQLite 마이그레이션 러너와 `publish` 파이프라인을 구현했다.
6. 실제 `content.db` 에 영어/일본어 사전 데이터를 적재하고 검색 인덱스를 재생성했다.
7. `KANJIDIC2`, `JLPT`, `Oxford`, `Tatoeba jpn-eng` ETL 을 추가했다.
8. `apps/desktop` 에 Solid + Tauri 검색 앱을 초기화하고 `content.db` read-only 검색을 연결했다.
9. `generate-courses`, `quality-report` CLI 를 추가했다.
10. `progress.db` 를 Tauri study session / review command 와 연결했다.
11. 검색 UI를 lexeme detail, examples, kanji inspector, due review queue 로 확장했다.
12. 코스 선택이 실제 학습 큐와 유닛 진행도에 반영되도록 연결했다.
13. 객관식 / 카드식 / 짝맞추기 기반의 데스크탑 학습 흐름을 추가했다.
14. 일본어 유치원 코스를 히라가나 / 가타카나 스타터로 나누고 코스 맵을 추가했다.
15. 홈 화면과 학습 화면을 분리하고 모든 학습 설명을 한국어 중심으로 정리했다.
16. 단어 학습, 단어 퀴즈, 문장 학습을 분리한 흐름으로 정리했다.
17. 로컬 LLM provider 설정을 추가하고 단어 예문을 새로 생성할 수 있게 했다.
18. 기기 TTS로 단어/문장 발음을 바로 들을 수 있게 했다.
19. 현재 머신의 DB와 Ollama를 다른 기기에서도 쓰도록 `crates/linguaforge-api` HTTP 서버를 추가했다.
20. 프론트엔드 API 레이어를 로컬 Tauri invoke 와 원격 HTTP 호출을 모두 지원하도록 바꿨다.
21. Android 프로젝트를 초기화하고 디버그 APK/AAB 빌드까지 확인했다.

## 다음 우선순위

1. 원격 연결 상태 확인 버튼과 실패 원인 안내 추가
2. API 인증/토큰과 LAN 외부 노출 가이드 정리
3. 문장형 / 받아쓰기 / 듣기형 문제로 학습 모드 확장
4. AI enrichment queue 와 quality feedback loop 설계
5. progress sync 정책과 모바일 흐름 정리
