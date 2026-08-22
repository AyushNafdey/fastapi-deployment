from datetime import datetime
from pydantic import BaseModel

class MongoDBSchema(BaseModel):
    timestamp: datetime
    expiry: datetime
    total_ce_oi: int
    total_pe_oi: int
    ce_oi_change: int
    pe_oi_change: int
