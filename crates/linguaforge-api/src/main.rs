use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Path, Query},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use clap::Parser;
use linguaforge_service::{
    GeneratedLexemeFeedbackResult, JapaneseBoosterPack, JapaneseBoosterRecommendation,
    KoreanMeaningHint, LlmConnectionStatus, LlmProviderSettings, GeneratedSentenceLesson,
    ensure_korean_meanings_api, finish_study_session_api, generate_japanese_booster_pack_api,
    generate_sentence_lesson_api, get_course_map_api, get_dashboard_snapshot_api, get_due_reviews_api,
    get_lexeme_detail_api, get_llm_settings_api, get_study_starts_api,
    recommend_japanese_booster_api, save_llm_settings_api, search_lexemes_api,
    start_study_session_api, submit_generated_lexeme_feedback_api,
    submit_lexeme_review_api, test_llm_settings_api,
};
use serde::Deserialize;
use serde_json::json;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tracing::info;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "0.0.0.0")]
    host: String,
    #[arg(long, default_value_t = 8787)]
    port: u16,
}

#[derive(Debug)]
struct ApiError(String);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SearchParams {
    query: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReviewParams {
    course_key: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StartSessionRequest {
    mode: Option<String>,
    course_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FinishSessionRequest {
    session_id: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubmitReviewRequest {
    session_id: i64,
    lexeme_id: i64,
    grade: String,
    response_time_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GenerateSentenceRequest {
    lexeme_id: i64,
    support_lexeme_ids: Option<Vec<i64>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KoreanMeaningRequest {
    lexeme_ids: Vec<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TestLlmRequest {
    settings: Option<LlmProviderSettings>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GenerateJapanesePackRequest {
    profile_key: Option<String>,
    theme_key: Option<String>,
    count: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeneratedLexemeFeedbackRequest {
    lexeme_id: i64,
    profile_key: Option<String>,
    theme_key: Option<String>,
    rating: String,
}

impl From<String> for ApiError {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": self.0 })),
        )
            .into_response()
    }
}

async fn run_blocking<T, F>(operation: F) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|err| ApiError(format!("blocking task failed: {err}")))?
        .map_err(ApiError::from)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let args = Args::parse();

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/dashboard", get(dashboard))
        .route("/api/study-starts", get(study_starts))
        .route("/api/course-map/{course_key}", get(course_map))
        .route("/api/search", get(search))
        .route("/api/lexemes/{lexeme_id}", get(lexeme_detail))
        .route("/api/session/start", post(start_session))
        .route("/api/session/finish", post(finish_session))
        .route("/api/reviews", get(reviews))
        .route("/api/reviews/lexeme", post(submit_review))
        .route("/api/llm-settings", get(llm_settings).put(save_settings))
        .route("/api/llm-settings/test", post(test_llm))
        .route("/api/sentences/generate", post(generate_sentence))
        .route("/api/lexemes/korean-meanings", post(generate_korean_meanings))
        .route("/api/lexemes/japanese-booster-pack", post(generate_japanese_pack))
        .route(
            "/api/lexemes/japanese-booster-recommendation",
            get(recommend_japanese_pack),
        )
        .route("/api/lexemes/generated-feedback", post(submit_generated_feedback))
        .layer(CorsLayer::very_permissive());

    let address = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&address).await?;
    info!(address = %address, "LinguaForge API server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn dashboard() -> Result<Json<impl serde::Serialize>, ApiError> {
    Ok(Json(get_dashboard_snapshot_api()?))
}

async fn study_starts() -> Result<Json<impl serde::Serialize>, ApiError> {
    Ok(Json(get_study_starts_api()?))
}

async fn course_map(Path(course_key): Path<String>) -> Result<Json<impl serde::Serialize>, ApiError> {
    Ok(Json(get_course_map_api(course_key)?))
}

async fn search(Query(params): Query<SearchParams>) -> Result<Json<impl serde::Serialize>, ApiError> {
    Ok(Json(search_lexemes_api(params.query.unwrap_or_default(), params.limit)?))
}

async fn lexeme_detail(Path(lexeme_id): Path<i64>) -> Result<Json<impl serde::Serialize>, ApiError> {
    Ok(Json(get_lexeme_detail_api(lexeme_id)?))
}

async fn start_session(
    Json(payload): Json<StartSessionRequest>,
) -> Result<Json<impl serde::Serialize>, ApiError> {
    Ok(Json(start_study_session_api(payload.mode, payload.course_key)?))
}

async fn finish_session(
    Json(payload): Json<FinishSessionRequest>,
) -> Result<Json<impl serde::Serialize>, ApiError> {
    Ok(Json(finish_study_session_api(payload.session_id)?))
}

async fn reviews(Query(params): Query<ReviewParams>) -> Result<Json<impl serde::Serialize>, ApiError> {
    Ok(Json(get_due_reviews_api(params.course_key, params.limit)?))
}

async fn submit_review(
    Json(payload): Json<SubmitReviewRequest>,
) -> Result<Json<impl serde::Serialize>, ApiError> {
    Ok(Json(submit_lexeme_review_api(
        payload.session_id,
        payload.lexeme_id,
        payload.grade,
        payload.response_time_ms,
    )?))
}

async fn llm_settings() -> Result<Json<LlmProviderSettings>, ApiError> {
    Ok(Json(get_llm_settings_api()?))
}

async fn save_settings(
    Json(settings): Json<LlmProviderSettings>,
) -> Result<Json<LlmProviderSettings>, ApiError> {
    Ok(Json(save_llm_settings_api(settings)?))
}

async fn test_llm(
    Json(payload): Json<TestLlmRequest>,
) -> Result<Json<LlmConnectionStatus>, ApiError> {
    Ok(Json(run_blocking(move || test_llm_settings_api(payload.settings)).await?))
}

async fn generate_sentence(
    Json(payload): Json<GenerateSentenceRequest>,
) -> Result<Json<GeneratedSentenceLesson>, ApiError> {
    Ok(Json(
        run_blocking(move || {
            generate_sentence_lesson_api(payload.lexeme_id, payload.support_lexeme_ids)
        })
        .await?,
    ))
}

async fn generate_korean_meanings(
    Json(payload): Json<KoreanMeaningRequest>,
) -> Result<Json<Vec<KoreanMeaningHint>>, ApiError> {
    Ok(Json(
        run_blocking(move || ensure_korean_meanings_api(payload.lexeme_ids)).await?,
    ))
}

async fn generate_japanese_pack(
    Json(payload): Json<GenerateJapanesePackRequest>,
) -> Result<Json<JapaneseBoosterPack>, ApiError> {
    Ok(Json(
        run_blocking(move || {
            generate_japanese_booster_pack_api(payload.profile_key, payload.theme_key, payload.count)
        })
        .await?,
    ))
}

async fn recommend_japanese_pack() -> Result<Json<JapaneseBoosterRecommendation>, ApiError> {
    Ok(Json(run_blocking(recommend_japanese_booster_api).await?))
}

async fn submit_generated_feedback(
    Json(payload): Json<GeneratedLexemeFeedbackRequest>,
) -> Result<Json<GeneratedLexemeFeedbackResult>, ApiError> {
    Ok(Json(
        run_blocking(move || {
            submit_generated_lexeme_feedback_api(
                payload.lexeme_id,
                payload.profile_key,
                payload.theme_key,
                payload.rating,
            )
        })
        .await?,
    ))
}
