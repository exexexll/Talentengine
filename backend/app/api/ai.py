from fastapi import APIRouter, HTTPException
from fastapi import Query
from pydantic import BaseModel

from backend.app.services.ai_research import research_geography

router = APIRouter()


class AIResearchSource(BaseModel):
    title: str
    url: str


class AIResearchResponse(BaseModel):
    geography_id: str
    scenario_id: str
    geography_name: str
    summary: str
    sources: list[AIResearchSource]
    news_score_adjustment: float = 0.0
    cached: bool


@router.get("/{geography_id}", response_model=AIResearchResponse)
def get_ai_research(
    geography_id: str,
    scenario_id: str = Query(default="default-opportunity"),
) -> AIResearchResponse:
    try:
        result = research_geography(geography_id, scenario_id=scenario_id)
    except Exception as exc:
        print(f"[AI Research] Error for {geography_id}: {exc}")
        return AIResearchResponse(
            geography_id=geography_id,
            scenario_id=scenario_id,
            geography_name=geography_id,
            summary=(
                f"AI research is temporarily unavailable for this geography.\n\n"
                f"**Reason:** {exc}\n\n"
                f"*Configure `SERPAPI_KEY` and `OPENAI_API_KEY` in `.env` for full AI briefings.*"
            ),
            sources=[],
            news_score_adjustment=0.0,
            cached=False,
        )
    return AIResearchResponse(**result)
