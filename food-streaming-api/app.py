from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import pandas as pd
import logging
import traceback

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# CSV location
CSV_URL = "https://s3-food-data-test.s3.us-east-1.amazonaws.com/total_data.csv"

@app.get('/')
async def root():
    return {
        "message": "API is running (streaming mode - memory safe)",
        "endpoints": {
            "/fetch_data": "GET with params: year, country, market, limit",
            "/debug/health": "Health check",
            "/debug/sample": "Preview first N rows"
        }
    }

@app.get('/debug/health')
async def health():
    """Simple health check - no CSV loading"""
    return {"status": "healthy", "mode": "streaming"}

@app.get('/debug/sample')
async def get_sample(rows: int = Query(5, le=100)):
    """Preview first few rows without heavy memory use"""
    try:
        df_sample = pd.read_csv(CSV_URL, nrows=rows)
        return {
            "columns": list(df_sample.columns),
            "sample_data": df_sample.fillna('').to_dict(orient='records')
        }
    except Exception as e:
        logger.error(f"Sample error: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get('/fetch_data')
async def fetch_data_api(
    year: int = Query(None),
    country: str = Query(None),
    market: str = Query(None),
    limit: int = Query(100, le=5000)
):
    """
    Stream CSV in chunks, filter, return results.
    Never loads full file into memory.
    """
    try:
        logger.info(f"Request: year={year}, country={country}, market={market}")
        
        chunk_size = 10000
        matching_chunks = []
        rows_collected = 0
        
        # Read CSV in chunks
        for chunk in pd.read_csv(CSV_URL, chunksize=chunk_size, low_memory=False):
            # Apply filters
            if year is not None and 'year' in chunk.columns:
                chunk = chunk[chunk['year'] == year]
            if country is not None and 'country' in chunk.columns:
                chunk = chunk[chunk['country'].astype(str).str.contains(country, case=False, na=False)]
            if market is not None and 'mkt_name' in chunk.columns:
                chunk = chunk[chunk['mkt_name'].astype(str).str.contains(market, case=False, na=False)]
            
            if not chunk.empty:
                matching_chunks.append(chunk)
                rows_collected += len(chunk)
                
                # Stop once we have enough
                if rows_collected >= limit:
                    break
        
        if not matching_chunks:
            return JSONResponse(
                content={"error": "No data found", "records": 0},
                status_code=404
            )
        
        # Combine results (only the filtered rows, not full file)
        result_df = pd.concat(matching_chunks, ignore_index=True).head(limit)
        
        return JSONResponse(content={
            "total": len(result_df),
            "filters": {"year": year, "country": country, "market": market},
            "data": result_df.fillna('').to_dict(orient='records')
        })
        
    except Exception as e:
        logger.error(traceback.format_exc())
        return JSONResponse(content={"error": str(e)}, status_code=500)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8080)
