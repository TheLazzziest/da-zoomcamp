import abc

import pydantic
from pydantic_extra_types import pendulum_dt as pendulum


class NYCRecordSchema(pydantic.BaseModel, abc.ABC):
    VendorID: pydantic.PositiveFloat
    RatecodeID: pydantic.PositiveInt
    extra: pydantic.PositiveFloat
    tolls_amount: pydantic.PositiveFloat
    trip_distance: pydantic.PositiveFloat
    PULocationID: pydantic.PositiveFloat
    DOLocationID: pydantic.PositiveInt
    passenger_count: pydantic.PositiveFloat
    total_amount: pydantic.PositiveFloat
    fare_amount: pydantic.PositiveFloat
    improvement_surcharge: pydantic.PositiveFloat
    cbd_congestion_fee: pydantic.PositiveFloat
    congestion_surcharge: pydantic.PositiveFloat
    mta_tax: pydantic.PositiveFloat
    store_and_fwd_flag: pydantic.PositiveFloat
    payment_type: pydantic.PositiveInt
    tip_amount: pydantic.PositiveFloat

    model_config = pydantic.ConfigDict()


class RecordGreenTrip(NYCRecordSchema):
    """
    A schema definition of a trip data source for green taxis
    """

    lpep_pickup_datetime: pendulum.DateTime
    lpep_dropoff_datetime: pendulum.DateTime
    trip_type: pydantic.PositiveInt
    ehale_fee: pydantic.PositiveFloat


class RecordYellowTrip(NYCRecordSchema):
    """
    A schema definition of a trip data source for yellow taxis
    """

    tpep_pickup_datetime: pendulum.DateTime
    tpep_dropoff_datetime: pendulum.DateTime
    Airport_fee: pydantic.PositiveFloat


class RecordFHVTrip(NYCRecordSchema):
    """
    A schema definition of a trip data source for fhv taxis
    """


class RecordFHVHVTrip(NYCRecordSchema):
    """
    A schema definition of a trip data source for fhvhv taxis
    """
