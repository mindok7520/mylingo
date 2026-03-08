# LinguaForge Architecture v3

## 핵심 개선 사항

기존 계획은 기능적으로 충분했지만, 모바일 성능과 동기화 안정성을 더 높이려면 DB와 파이프라인을 조금 더 분리하는 편이 좋다.

### 1. DB를 `content.db` 와 `progress.db` 로 분리

- `content.db`: ETL 결과물, 읽기 중심, 거의 불변 데이터
- `progress.db`: SRS, 학습 기록, 설정, 코스 진행률
- 장점
  - 모바일에서 대용량 사전 데이터와 자주 쓰는 학습 기록 I/O를 분리할 수 있다.
  - Syncthing 동기화 시 충돌 범위를 `progress.db` 위주로 줄일 수 있다.
  - ETL 재생성 시 `content.db` 만 교체 가능하다.

### 2. JSON 컬럼 축소, 정규화 확대

기존 `words.tags`, `words.source_ids`, `kanji.meanings_en` 같은 JSON 텍스트 컬럼은 간단하지만 다음 문제가 있다.

- 반복 문자열로 DB 크기 증가
- 부분 업데이트 비용 증가
- 인덱싱과 필터링이 약함

대신 아래처럼 분리한다.

- `lexeme_senses`
- `lexeme_tag_map`
- `sense_sources`
- `example_translations`
- `kanji_readings`, `kanji_meanings`

### 3. 학습 대상 식별 방식 단순화

기존 `srs_state` 는 `word_id`, `kanji_id`, `grammar_id` 3개 nullable FK를 가진다. 이 구조는 제약도 많고 인덱스도 비효율적이다.

대신:

- `review_items(item_type, item_id)`
- `srs_state(review_item_id)`
- `review_events(review_item_id)`

이렇게 통합하면 단어/한자/문법 모두 같은 SRS 엔진으로 처리할 수 있다.

### 4. 검색 전용 FTS 테이블 분리

- 메인 테이블에 모든 검색 요구를 억지로 넣지 않는다.
- `lexeme_search`, `example_search` FTS5 테이블을 별도로 유지한다.
- ETL 단계에서 배치 생성하면 앱 런타임 비용이 낮다.

### 5. ETL은 staging → merge → publish 3단계로 분리

- `raw/`: 원본 다운로드
- `staging/`: 파서가 만든 중간 산출물
- `publish/`: 최종 `content.db`

이렇게 두면 파서 재실행, 검증, 병합 전략 실험이 쉬워진다.

### 6. 대용량 예문은 먼저 필터링 후 매핑

Tatoeba 전체를 바로 단어-예문 매핑하면 메모리와 시간이 크게 든다.

권장 방식:

1. 언어별 문장 원본 추출
2. 길이/품질/라이선스 필터
3. 필요한 언어 쌍만 translation join
4. 형태소 분석으로 lexeme 후보 추출
5. `example_candidates` staging 적재
6. 점수 기반 상위 예문만 publish

### 7. 모바일 최적화를 위한 데이터 배포 단위 분리

처음부터 모든 자산을 모바일에 넣기보다 프로필을 나눈다.

- `core`: 기초 어휘 + 기본 예문 + JLPT N5/N4 + CEFR A1/A2
- `extended`: 상위 코스, 대규모 예문
- `media`: KanjiVG, Kanji Alive 오디오

이 구조면 APK/AAB 크기와 초기 동기화 시간을 줄일 수 있다.

## 권장 디렉터리 구조

```text
config/
  sources.toml
data/
  raw/
  staging/
  publish/
docs/
sql/
crates/
  linguaforge-core/
  linguaforge-cli/
  linguaforge-etl/   # 다음 단계
  linguaforge-app/   # Tauri 다음 단계
```

## ETL 실행 단위

권장 CLI 흐름은 아래와 같다.

```text
fetch     : 원본 다운로드
extract   : 압축 해제 및 구조 정리
stage     : 공통 중간 포맷으로 변환
merge     : 중복 병합 및 score 계산
publish   : content.db 생성
verify    : 무결성/통계 검증
```

## 성능 기본 원칙

- XML/CSV/TSV 는 스트림 파싱 우선
- SQLite insert 는 5,000~20,000건 단위 batch transaction 사용
- join 테이블은 가능하면 `WITHOUT ROWID`
- 문자열 enum 은 lookup table 또는 small text set 유지
- 앱 조회용 쿼리는 `EXPLAIN QUERY PLAN` 기준으로 인덱스 설계
- 모바일에서는 읽기 트랜잭션 위주, ETL은 데스크탑 전용
