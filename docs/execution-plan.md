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
- [ ] `content.db` / `progress.db` 마이그레이션 러너 구현
- [ ] source import manifest / checksum / version lock 도입
- [ ] JMdict, NGSL 파서 1차 구현
- [ ] import 통계와 품질 리포트 출력

## Phase 2 - Language Content Build

- [ ] JLPT / CEFR / frequency merge 규칙 구현
- [ ] example 품질 점수화 구현
- [ ] course generator 1차 구현
- [ ] AI enrichment queue 설계

## Phase 3 - App Runtime

- [ ] Tauri 앱 초기화
- [ ] read-only content DB 연결
- [ ] progress DB 연결
- [ ] dashboard / course / search 기본 UI

## Phase 4 - Learning Engine

- [ ] SRS review item 모델 구현
- [ ] flashcard / multiple choice
- [ ] progress sync 정책 구현

## 이번 작업에서 실제로 진행한 것

1. DB를 content/progress 분리형으로 재설계했다.
2. 자동 데이터 수집용 Rust CLI를 만들었다.
3. 소스별 다운로드/압축 해제 설정을 `config/sources.toml` 로 외부화했다.

## 다음 우선순위

1. `fetch` 결과를 기준으로 staging manifest 생성
2. JMdict / NGSL 파서 연결
3. SQLite 마이그레이션 러너 추가
