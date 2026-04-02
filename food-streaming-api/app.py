from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import pandas as pd
import boto3
from io import StringIO
import uvicorn
import json

app = FastAPI()

@app.get('/')
async def root():
    return {
        "message": "API is running",
        "endpoints": {
            "/fetch_data": "GET with params: year, country, market",
            "example": "/fetch_data?year=2023&country=USA&market=Retail"
        }
    }

def fetch_data(year: int = None, country: str = None, market: str = None):
    try:
        # Load CSV content into a pandas DataFrame
        df = pd.read_csv("https://s3-food-data-test.s3.us-east-1.amazonaws.com/total_data.csv")
        
        print(f"Total rows loaded: {df.shape[0]}")
        
        # Apply filters based on provided parameters
        if year is not None:
            print(f"Filtering by year: {year}")
            df = df[df['year'] == year]
        if country is not None:
            print(f"Filtering by country: {country}")
            df = df[df['country'] == country]
        if market is not None:
            print(f"Filtering by market: {market}")
            df = df[df['mkt_name'] == market]

        # Fill NaN values with empty strings
        df_filter = df.fillna('')

        print(f"Rows after filtering: {df_filter.shape[0]}")
        
        # Convert filtered DataFrame to JSON
        if df_filter.empty:
            return {'error': 'No data found for the specified filters.'}
        else:
            filtered_json = df_filter.to_json(orient='records')
            return filtered_json

    except Exception as e:
        return {'error': str(e)}

@app.get('/fetch_data')
async def fetch_data_api(year: int = Query(None), country: str = Query(None), market: str = Query(None)):
    filtered_data = fetch_data(year, country, market)
    
    # Check if error was returned
    if isinstance(filtered_data, dict) and 'error' in filtered_data:
        return JSONResponse(content=filtered_data, status_code=404)
    
    # Return successful response
    return JSONResponse(content=json.loads(filtered_data))

if __name__ == '__main__':
    uvicorn.run(app, port=8080, host='0.0.0.0')
