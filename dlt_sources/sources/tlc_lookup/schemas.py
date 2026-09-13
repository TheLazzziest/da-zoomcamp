import pydantic


class ZoneLookupRecord(pydantic.BaseModel):
    """A TLC taxi zone lookup row: LocationID to borough/zone/service-zone mapping."""

    location_id: int
    borough: str
    zone: str
    service_zone: str

    @classmethod
    def primary_key(cls) -> tuple[str, ...]:
        return ("location_id",)

    model_config = pydantic.ConfigDict()
