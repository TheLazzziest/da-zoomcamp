import pydantic
from pydantic_extra_types import pendulum_dt as pendulum


class ServiceRequestRecord(pydantic.BaseModel):
    """An NYC 311 service request row."""

    unique_key: str
    created_date: pendulum.DateTime
    closed_date: pendulum.DateTime | None = None
    due_date: pendulum.DateTime | None = None
    resolution_action_updated_date: pendulum.DateTime | None = None
    agency: str | None = None
    agency_name: str | None = None
    complaint_type: str | None = None
    descriptor: str | None = None
    status: str | None = None
    borough: str | None = None
    community_board: str | None = None
    police_precinct: str | None = None
    incident_zip: str | None = None
    city: str | None = None
    address_type: str | None = None
    street_name: str | None = None
    cross_street_1: str | None = None
    cross_street_2: str | None = None
    landmark: str | None = None
    facility_type: str | None = None
    location_type: str | None = None
    open_data_channel_type: str | None = None
    resolution_description: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    x_coordinate_state_plane: float | None = None
    y_coordinate_state_plane: float | None = None

    @classmethod
    def primary_key(cls) -> tuple[str, ...]:
        return ("unique_key",)

    model_config = pydantic.ConfigDict()
