# main.py for Cloud Functions
from shopify_etl import ShopifyETLPipeline
import functions_framework
import json
import os

@functions_framework.http
def shopify_etl_function(request):
    """HTTP Cloud Function that runs the Shopify ETL pipeline.
    Args:
        request (flask.Request): The request object.
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`.
    """
    # Parse request parameters (if any)
    request_json = request.get_json(silent=True)
    
    # Default parameters
    force_full_load = False
    test_mode = False
    max_pages = 5000
    
    # Override with request parameters if provided
    if request_json:
        force_full_load = request_json.get('force_full_load', force_full_load)
        test_mode = request_json.get('test_mode', test_mode)
        max_pages = request_json.get('max_pages', max_pages)
    
    # Or use environment variables if set
    force_full_load = os.getenv('FORCE_FULL_LOAD', '').lower() == 'true' or force_full_load
    test_mode = os.getenv('TEST_MODE', '').lower() == 'true' or test_mode
    max_pages_env = os.getenv('MAX_PAGES')
    if max_pages_env and max_pages_env.isdigit():
        max_pages = int(max_pages_env)
    
    try:
        # Initialize and run the pipeline
        pipeline = ShopifyETLPipeline()
        pipeline.execute(
            force_full_load=force_full_load,
            test_mode=test_mode,
            max_pages=max_pages
        )
        
        return json.dumps({
            "status": "success",
            "message": "Shopify ETL pipeline executed successfully",
            "params": {
                "force_full_load": force_full_load,
                "test_mode": test_mode,
                "max_pages": max_pages
            }
        })
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        
        return json.dumps({
            "status": "error",
            "message": str(e),
            "details": error_details,
            "params": {
                "force_full_load": force_full_load,
                "test_mode": test_mode,
                "max_pages": max_pages
            }
        }), 500  # HTTP 500 status code