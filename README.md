# LinguaForge

Tauri + Rust + SolidJS 기반의 개인용 언어 학습 앱 프로젝트다.

현재는 대용량 공개 언어 데이터를 안정적으로 수집하고 정리하기 위한 자동화 기반부터 구축하고 있다.

## 현재 구현된 범위

- Rust 워크스페이스 구성
- 데이터 소스 레지스트리(`config/sources.toml`)
- 자동 다운로드/압축 해제 CLI (`cargo run -p linguaforge-cli -- fetch --all`)
- staging 파이프라인 (`google-10000`, `NGSL`, `JMdict`, `KANJIDIC2`, `JLPT`, `Oxford`, `Tatoeba jpn-eng`)
- SQLite 마이그레이션 러너 (`content.db`, `progress.db`)
- staged JSONL -> `content.db` publish 파이프라인
- `kanji`, `examples`, `lexeme_examples`, `kanji_lexemes` 적재 지원
- Solid + Tauri v2 데스크탑 앱 초기화 (`apps/desktop`)
- read-only `content.db` 검색/상세 조회 + `progress.db` 학습 세션 연결
- 수준별 시작 코스 + 유치원 스타터 코스(`히라가나`, `가타카나`, `영어 기초`) 제공
- 객관식 / 카드식 / 짝맞추기 기반의 데스크탑 학습 흐름
- 코스 맵과 유닛 진행도 표시
- auto course 생성 CLI (`generate-courses`)
- 데이터 커버리지 품질 리포트 CLI (`quality-report`)
- 개선된 아키텍처/DB 설계 문서
- content/progress 분리형 SQLite 초안 스키마

## 빠른 시작

```bash
cargo run -p linguaforge-cli -- sources list
cargo run -p linguaforge-cli -- fetch google-10000 ngsl jmdict kanjidic2 jlpt-vocabulary oxford-word-list tatoeba-jpn-eng
cargo run -p linguaforge-cli -- stage google-10000 ngsl jmdict kanjidic2 jlpt-vocabulary oxford-word-list tatoeba-jpn-eng
cargo run -p linguaforge-cli -- migrate
cargo run -p linguaforge-cli -- publish google-10000 ngsl jmdict kanjidic2 jlpt-vocabulary oxford-word-list tatoeba-jpn-eng
cargo run -p linguaforge-cli -- generate-courses --replace --unit-size 20
cargo run -p linguaforge-cli -- quality-report
```

## 데스크탑 앱

```bash
cd apps/desktop
npm install
npm run build
npm run tauri dev
```

필요하면 `LINGUAFORGE_CONTENT_DB=/absolute/path/to/content.db` 와 `LINGUAFORGE_PROGRESS_DB=/absolute/path/to/progress.db` 환경변수로 DB 경로를 지정할 수 있다.

## 다음 단계

1. JLPT / CEFR / frequency merge 규칙 정교화
2. 문법/코스 자동 생성 범위 확장
3. 문장형 문제 / 받아쓰기 / 스펠링 등 문제 타입 확장
4. 모바일 학습 흐름과 progress sync 정책 설계
