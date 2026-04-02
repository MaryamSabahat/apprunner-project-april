from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import pandas as pd
import logging
import traceback

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

CSV_URL = "https://s3-food-data-test.s3.us-east-1.amazonaws.com/total_data.csv"

@app.get('/')
async def root():
    return {
        "message": "API running in streaming mode - memory safe",
        "endpoints": {
            "/fetch_data": "Filter data (streams CSV in chunks)",
            "/debug/health": "Health check",
            "/debug/countries": "List available countries"
        }
    }

@app.get('/debug/health')
async def health():
    return {"status": "healthy", "mode": "streaming"}

@app.get('/debug/countries')
async def list_countries():
    """Get unique countries without loading full CSV"""
    try:
        # Read only the country column in chunks
        countries = set()
        for chunk in pd.read_csv(CSV_URL, chunksize=10000, usecols=['country']):
            countries.update(chunk['country'].dropna().unique())
        
        return {"countries": sorted(list(countries))[:50]}  # First 50
    except Exception as e:
        return {"error": str(e)}

@app.get('/fetch_data')
async def fetch_data_api(
    year: int = Query(None),
    country: str = Query(None),
    market: str = Query(None),
    limit: int = Query(100, le=5000)
):
    """
    Stream CSV in chunks, filter, return results.
    Memory usage stays under 500MB regardless of CSV size.
    """
    try:
        logger.info(f"Request: year={year}, country={country}, market={market}")
        
        chunk_size = 10000
        matching_rows = []
        total_matched = 0
        
        # Define columns we need (optional - remove to get all)
        # usecols = ['country', 'mkt_name', 'year', 'month']  # Uncomment to load only specific columns
        
        for chunk_idx, chunk in enumerate(pd.read_csv(CSV_URL, chunksize=chunk_size, low_memory=False)):
            # Apply filters
            if year is not None and 'year' in chunk.columns:
                chunk = chunk[chunk['year'] == year]
            
            if country is not None and 'country' in chunk.columns:
                chunk = chunk[chunk['country'].astype(str).str.contains(country, case=False, na=False)]
            
            if market is not None and 'mkt_name' in chunk.columns:
                chunk = chunk[chunk['mkt_name'].astype(str).str.contains(market, case=False, na=False)]
            
            if not chunk.empty:
                matching_rows.append(chunk)
                total_matched += len(chunk)
                logger.info(f"Chunk {chunk_idx}: found {len(chunk)} matches (total: {total_matched})")
                
                # Stop once we have enough results
                if total_matched >= limit:
                    break
        
        if not matching_rows:
            return JSONResponse(
                content={"error": "No data found", "records": 0},
                status_code=404
            )
        
        # Combine only the matching rows (small result set)
        result_df = pd.concat(matching_rows, ignore_index=True)
        
        # Limit to requested number
        if len(result_df) > limit:
            result_df = result_df.head(limit)
        
        logger.info(f"Returning {len(result_df)} records")
        
        return JSONResponse(content={
            "total": len(result_df),
            "filters": {"year": year, "country": country, "market": market},
            "data": result_df.fillna('').to_dict(orient='records')
        })
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            content={"error": str(e), "trace": traceback.format_exc()},
            status_code=500
        )

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8080)
