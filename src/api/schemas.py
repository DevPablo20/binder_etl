from typing import Literal

from pydantic import BaseModel, ConfigDict

ObjectType = Literal["account", "campaign", "ad_group", "ad"]


class CatalogItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    platform: str
    object_type: ObjectType
    account_id: str
    account_name: str
    campaign_id: str | None = None
    campaign_name: str | None = None
    ad_group_id: str | None = None
    ad_group_name: str | None = None
    ad_id: str | None = None
    ad_name: str | None = None
