# LinguaForge

Tauri + Rust + SolidJS 기반의 개인용 언어 학습 앱 프로젝트다.

현재는 대용량 공개 언어 데이터를 안정적으로 수집하고 정리하는 ETL, SQLite 기반 학습 상태 관리, 한국어 중심 학습 UI, 로컬/원격 학습 서버 흐름까지 이어지는 로컬 우선 언어 학습 스택을 구축하고 있다.

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
- 홈/학습 페이지 분리 + 한국어 중심 학습 UI
- 단어 학습 / 단어 퀴즈 / 문장 학습 분리 흐름
- 로컬 LLM provider 설정과 새 예문 생성
- 기기 TTS 기반 발음 재생
- 현재 머신의 `content.db` / `progress.db` / Ollama를 외부 기기에서 쓰기 위한 API 서버(`linguaforge-api`)
- Android init/build 스크립트와 디버그 빌드 검증
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

DB 기본 경로는 `db/content.db`, `db/progress.db` 다. 필요하면 `LINGUAFORGE_CONTENT_DB=/absolute/path/to/content.db`, `LINGUAFORGE_PROGRESS_DB=/absolute/path/to/progress.db` 로 바꿀 수 있다.

## 데스크탑 앱

```bash
npm install --prefix apps/desktop
npm run build --prefix apps/desktop
npm run tauri dev --prefix apps/desktop
```

앱 메인은 한국어 안내를 기준으로 구성되어 있고, 학습은 `단어 학습`, `단어 퀴즈`, `문장 학습`으로 나뉜다. 문장 학습에서는 기본값으로 `http://127.0.0.1:11434` 의 Ollama를 사용하며, 기기 TTS로 발음을 재생한다.

## API 서버

폰이나 다른 기기에서 현재 머신의 DB와 Ollama를 그대로 쓰려면 API 서버를 띄우면 된다.

```bash
cargo run -p linguaforge-api -- --host 0.0.0.0 --port 8787
```

- 기본 주소는 `http://<현재-머신-IP>:8787`
- 헬스 체크는 `GET /health`
- 앱에서 `학습 서버 주소`에 `http://<현재-머신-IP>:8787` 를 저장하면 원격 HTTP API 모드로 전환된다
- 서버도 `LINGUAFORGE_CONTENT_DB`, `LINGUAFORGE_PROGRESS_DB` 환경변수를 그대로 따른다
- LLM 설정은 앱의 `로컬 LLM` 패널에서 저장하며, 기본 provider 는 `ollama` 다

신뢰 가능한 로컬 네트워크나 Tailscale 같은 사설 네트워크에서만 쓰는 것을 권장한다. 현재 API 서버에는 별도 인증 토큰이 없다.

## Android

```bash
npm run android:init --prefix apps/desktop
npm run android:build --prefix apps/desktop -- --debug
```

- Android 프로젝트는 `apps/desktop/src-tauri/gen/android/` 아래에 생성된다
- 디버그 APK 는 `apps/desktop/src-tauri/gen/android/app/build/outputs/apk/universal/debug/app-universal-debug.apk` 에 생성된다
- 디버그 AAB 는 `apps/desktop/src-tauri/gen/android/app/build/outputs/bundle/universalDebug/app-universal-debug.aab` 에 생성된다
- Android 빌드가 되려면 JDK, Android SDK, NDK 가 설치되어 있어야 한다

## 다음 단계

1. 문장형 문제 / 받아쓰기 / 듣기형 문제 확장
2. API 서버 인증/토큰과 원격 연결 상태 확인 UX 보강
3. progress sync 정책과 모바일 운영 흐름 정리
4. 문법/코스 자동 생성 범위 확장
