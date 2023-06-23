from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from databases import Database
from sqlalchemy import create_engine, Column, Integer, String, DateTime, MetaData, Table, update
from datetime import datetime, timedelta
import os
import uvicorn

# Database configuration
DATABASE_URL = os.environ.get('DATABASE_URL', "postgresql://postgres:1234@localhost/Edge_DB")
database = Database(DATABASE_URL)
metadata = MetaData()

# Define the table schema
sources = Table(
    "sources",
    metadata,
    Column("source_id", Integer, primary_key=True),
    Column("source", String(200)),
    Column("source_type", String(10)),
    Column("source_tag", String(10)),
    Column("last_update_date", DateTime),
    Column("from_date", DateTime),
    Column("to_date", DateTime),
    Column("frequency", String(5))
)

# Create the FastAPI app
app = FastAPI()


# Model for request payload
class SourceCreate(BaseModel):
    source: str
    source_type: str
    source_tag: str
    from_date: datetime
    to_date: datetime
    last_update_date: datetime

# Model for request payload
class SourceUpdate(BaseModel):
    source_id: int
    from_date: datetime
    to_date: datetime
    last_update_date: datetime

# Model for response payload
class SourceData(BaseModel):
    source_id: int
    source: str = None
    source_type: str = None
    source_tag: str = None
    last_update_date: datetime = None
    from_date: datetime = None
    to_date: datetime = None
    frequency: str = None


# Endpoint to get all data for a given source
@app.get("/get_data")
async def get_source_data(source: SourceData):
    
    source_id = source.source_id
    query = sources.select().where(sources.c.source_id == source_id)
    result = await database.fetch_one(query)
    if result:
        source_data = SourceData(
            source_id=result["source_id"],
            source=result["source"],
            source_type=result["source_type"],
            source_tag=result["source_tag"],
            last_update_date=result["last_update_date"],
            from_date=result["from_date"],
            to_date=result["to_date"],
            frequency=result["frequency"],
        )
        return source_data
    else:
        raise HTTPException(status_code=404, detail="Source not found")



# Endpoint to trigger getting all data for a given source
@app.get("/get_data_trigger")
async def get_data_trigger(source: SourceData):

    source_id = source.source_id
    query = sources.select().where(sources.c.source_id == source_id)
    result = await database.fetch_one(query)
    if result:
        from_date = result["from_date"]
        to_date = result["to_date"]
        frequency = result["frequency"][:-1]
        frequency = int(frequency)
        
        adjusted_from_date = from_date + timedelta(minutes=frequency)
        adjusted_to_date = to_date + timedelta(minutes=frequency)
        
        source_data = SourceData(
            source_id=result["source_id"],
            source=result["source"],
            source_type=result["source_type"],
            source_tag=result["source_tag"],
            last_update_date=result["last_update_date"],
            from_date=adjusted_from_date,
            to_date=adjusted_to_date,
            frequency=result["frequency"],
        )
        return source_data
    else:
        raise HTTPException(status_code=404, detail="Source not found")

# Endpoint to update from_date, to_date, and last_update_date for a given source
@app.patch("/update_data")
async def update_source_data(source_update: SourceUpdate):
    source_id = source_update.source_id
    query = (
        update(sources)
        .where(sources.c.source_id == source_id)
        .values(
            from_date=source_update.from_date,
            to_date=source_update.to_date,
            last_update_date=source_update.last_update_date
        )
        .returning(sources.c.source_id)
    )
    result = await database.execute(query)
    if result:
        return {"status": "success"}
    else:
        raise HTTPException(status_code=404, detail="Source not found")


# Endpoint to create a new source
@app.post("/add_data")
async def create_source(source: SourceCreate):

    freq = source.to_date - source.from_date
    print(source.to_date,source.from_date)
    freq = int(freq.total_seconds() // 60)
    print(freq)
    freq = str(freq) + "M"
    query = sources.insert().values(
        source=source.source,
        source_type=source.source_type,
        source_tag=source.source_tag,
        last_update_date=source.last_update_date,
        from_date=source.from_date,
        to_date=source.to_date,
        frequency= freq
    )
    await database.execute(query)
    return {"status": "success"}


# Main function to connect to the database and run the app
@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


if __name__ == "__main__":
    engine = create_engine(DATABASE_URL)
    metadata.create_all(engine)
    uvicorn.run(app, host="localhost", port=1234)
