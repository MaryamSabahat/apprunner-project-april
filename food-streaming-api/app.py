from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import duckdb
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

CSV_URL = "https://s3-food-data-test.s3.us-east-1.amazonaws.com/total_data.csv"

@app.get('/')
async def root():
    return {
        "message": "Food Data API (Memory Efficient)",
        "endpoints": {
            "/fetch_data": "Filter by year, country, market",
            "/health": "Health check"
        }
    }

@app.get('/health')
async def health():
    return {"status": "healthy"}

@app.get('/fetch_data')
async def fetch_data(
    year: int = Query(None, description="Filter by year"),
    country: str = Query(None, description="Filter by country name"),
    market: str = Query(None, description="Filter by market name"),
    limit: int = Query(100, ge=1, le=5000, description="Max records to return")
):
    """
    Query CSV directly without loading into memory.
    DuckDB streams the file and only processes what's needed.
    """
    try:
        # Create in-memory database (very lightweight)
        conn = duckdb.connect(':memory:')
        
        # Build query - DuckDB reads CSV on the fly
        query = f"SELECT * FROM read_csv_auto('{CSV_URL}') WHERE 1=1"
        
        if year:
            query += f" AND year = {year}"
        
        if country:
            query += f" AND country ILIKE '%{country}%'"
        
        if market:
            query += f" AND mkt_name ILIKE '%{market}%'"
        
        query += f" LIMIT {limit}"
        
        logger.info(f"Executing query: {query}")
        
        # Execute and get results as pandas DataFrame (only results, not full file)
        result_df = conn.execute(query).df()
        conn.close()
        
        if result_df.empty:
            return JSONResponse(
                content={"error": "No data found for filters", "records": 0},
                status_code=404
            )
        
        # Convert to records (only the filtered results)
        records = result_df.fillna('').to_dict(orient='records')
        
        return {
            "total": len(records),
            "filters": {"year": year, "country": country, "market": market},
            "data": records
        }
        
    except Exception as e:
        logger.error(f"Query failed: {str(e)}")
        return JSONResponse(
            content={"error": f"Query failed: {str(e)}"},
            status_code=500
        )

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8080)
