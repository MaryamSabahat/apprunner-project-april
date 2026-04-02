from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import pandas as pd
import json
import uvicorn
import logging
import traceback

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

@app.get('/')
async def root():
    return {
        "message": "API is running",
        "endpoints": {
            "/fetch_data": "GET with params: year, country, market",
            "/debug/columns": "Check CSV columns and first row",
            "/debug/health": "Simple health check"
        }
    }

@app.get('/debug/health')
async def health():
    """Simple health check"""
    return {"status": "healthy", "service": "running"}

@app.get('/debug/columns')
async def check_columns():
    """Check what columns exist in the CSV"""
    try:
        logger.info("Checking CSV columns...")
        df = pd.read_csv("https://s3-food-data-test.s3.us-east-1.amazonaws.com/total_data.csv", nrows=5)
        return {
            "columns": list(df.columns),
            "row_count_preview": len(df),
            "first_row": df.iloc[0].to_dict() if len(df) > 0 else None
        }
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {"error": str(e), "traceback": traceback.format_exc()}

def fetch_data(year: int = None, country: str = None, market: str = None):
    try:
        logger.info(f"Loading CSV from S3...")
        df = pd.read_csv("https://s3-food-data-test.s3.us-east-1.amazonaws.com/total_data.csv")
        logger.info(f"CSV loaded. Rows: {len(df)}, Columns: {list(df.columns)}")
        
        # Apply filters
        if year is not None:
            logger.info(f"Filtering year={year}")
            if 'year' not in df.columns:
                return {'error': f'Column "year" not found. Available: {list(df.columns)}'}
            df = df[df['year'] == year]
            
        if country is not None:
            logger.info(f"Filtering country={country}")
            if 'country' not in df.columns:
                return {'error': f'Column "country" not found. Available: {list(df.columns)}'}
            df = df[df['country'] == country]
            
        if market is not None:
            logger.info(f"Filtering market={market}")
            if 'mkt_name' not in df.columns:
                return {'error': f'Column "mkt_name" not found. Available: {list(df.columns)}'}
            df = df[df['mkt_name'] == market]
        
        df_filter = df.fillna('')
        
        if df_filter.empty:
            return {'error': 'No data found for the specified filters.'}
        
        result = df_filter.to_json(orient='records')
        logger.info(f"Success! Returning {df_filter.shape[0]} records")
        return result
        
    except Exception as e:
        logger.error(f"ERROR in fetch_data: {str(e)}")
        logger.error(traceback.format_exc())
        return {'error': str(e)}

@app.get('/fetch_data')
async def fetch_data_api(year: int = Query(None), country: str = Query(None), market: str = Query(None)):
    logger.info(f"Request: year={year}, country={country}, market={market}")
    filtered_data = fetch_data(year, country, market)
    
    if isinstance(filtered_data, dict) and 'error' in filtered_data:
        return JSONResponse(content=filtered_data, status_code=404)
    
    return JSONResponse(content=json.loads(filtered_data))

if __name__ == '__main__':
    uvicorn.run(app, port=8080, host='0.0.0.0')
