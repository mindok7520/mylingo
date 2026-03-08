# LinguaForge

Tauri + Rust + SolidJS 기반의 개인용 언어 학습 앱 프로젝트다.

현재는 대용량 공개 언어 데이터를 안정적으로 수집하고 정리하기 위한 자동화 기반부터 구축하고 있다.

## 현재 구현된 범위

- Rust 워크스페이스 구성
- 데이터 소스 레지스트리(`config/sources.toml`)
- 자동 다운로드/압축 해제 CLI (`cargo run -p linguaforge-cli -- fetch --all`)
- 개선된 아키텍처/DB 설계 문서
- content/progress 분리형 SQLite 초안 스키마

## 빠른 시작

```bash
cargo run -p linguaforge-cli -- sources list
cargo run -p linguaforge-cli -- fetch google-10000
cargo run -p linguaforge-cli -- stage google-10000
```

## 다음 단계

1. ETL staging DB + importer 구현
2. JMdict / NGSL 파서 연결
3. content.db / progress.db 마이그레이션 시스템 추가
4. Tauri 앱 초기화 및 읽기 전용 content DB 연결
