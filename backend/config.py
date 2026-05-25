from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    google_cloud_project: str = ""
    vertex_location: str = "us-central1"
    google_genai_use_vertexai: bool = True

    gemini_model: str = "gemini-flash-latest"

    local_mode: bool = True
    stub_llm: bool = False
    chunk_seconds: int = 5
    dedupe_ttl_seconds: int = 60
    max_claims_per_session: int = 50
    # Direct-video mode checks every statement of the full transcript, so it
    # needs a much higher ceiling than the filtered audio mode.
    max_statements_per_session: int = 800
    # Cap concurrent evidence/verdict pipelines so a long transcript doesn't
    # fan out hundreds of grounded Gemini calls at once and trip rate limits.
    max_concurrent_checks: int = 5
    # Direct-video mode returns the whole transcript in one JSON response; give
    # it room so long videos aren't truncated mid-transcript.
    video_max_output_tokens: int = 8192

    firestore_collection: str = "verdicts"
    pubsub_chunk_topic: str = "fact-check-chunks"
    pubsub_verdict_topic: str = "fact-check-verdicts"

    trusted_domains: str = (
        "wikipedia.org,pubmed.ncbi.nlm.nih.gov,worldbank.org,"
        "earthquake.usgs.gov,cdc.gov,nih.gov,who.int"
    )

    host: str = "0.0.0.0"
    port: int = 8080

    @property
    def trusted_domain_list(self) -> list[str]:
        return [d.strip().lower() for d in self.trusted_domains.split(",") if d.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
