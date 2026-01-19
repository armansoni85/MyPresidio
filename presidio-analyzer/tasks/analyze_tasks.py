from celery import Celery, states
from presidio_analyzer import AnalyzerEngine, AnalyzerRequest
import json
from typing import List
import traceback

celery = Celery("presidio", broker="redis://localhost:6379", backend="redis://localhost:6379")

# Presidio analyzer engine
engine = AnalyzerEngine()

@celery.task(bind=True)
def analyze_task(self, req_data: dict):
    try:
        # Parse the request data
        req_data = AnalyzerRequest(req_data)
        if not req_data.text:
            raise Exception("No text provided")
        
        if not req_data.language:
            raise Exception("No language provided")
        
        # Call the Presidio analyzer engine
        recognizer_result_list = engine.analyze(
            text=req_data.text,
            language=req_data.language,
            correlation_id=req_data.correlation_id,
            score_threshold=req_data.score_threshold,
            entities=req_data.entities,
            return_decision_process=req_data.return_decision_process,
            ad_hoc_recognizers=req_data.ad_hoc_recognizers,
            context=req_data.context,
        )
        
        # Convert the recognizer result to a list of dictionaries (not JSON string)
        result = [r.to_dict() for r in recognizer_result_list]
        
        # Return the deserialized result (list of dictionaries)
        return result
    
    except Exception as ex:
        # Update task state with detailed error information
        self.update_state(
            state=states.FAILURE,
            meta={
                'exc_type': type(ex).__name__,
                'exc_message': traceback.format_exc().split('\n')
            }
        )
        raise ex
