import os
import json
from typing import List, Optional, Dict, Any
import requests
import logging 
from dotenv import load_dotenv
from utils import setup_logger

load_dotenv()
logger = setup_logger("speech_recognition")

# Define constants
DEFAULT_LOCALES = ["en-IN","hi-IN"]
API_VERSION = "2024-11-15"

def get_locales() -> List[str]:
    """
    Get speech recognition locales from environment variable or defaults.
    
    Returns:
        List[str]: List of locale codes
    """
    try:
        locales_str = os.getenv('SPEECH_LOCALES')
        if locales_str:
            return json.loads(locales_str)
        return DEFAULT_LOCALES
    except json.JSONDecodeError:
        logger.warning("Invalid JSON in SPEECH_LOCALES environment variable. Using defaults.")
        return DEFAULT_LOCALES

def build_api_url() -> str:
    """Build the Speech API URL."""
    region = os.getenv('SPEECH_REGION')
    if not region:
        raise ValueError("SPEECH_REGION environment variable is not set")
    return f"https://{region}.api.cognitive.microsoft.com/speechtotext/transcriptions:transcribe?api-version={API_VERSION}"

def recognize_from_file(filename: str) -> str:
    """
    Transcribe speech from an audio file.
    
    Args:
        filename (str): Path to the audio file
        
    Returns:
        str: Transcribed text or error message
    """
    if not os.path.exists(filename):
        return "Error: Audio file not found"

    try:
        url = build_api_url()
        headers = {
            "Ocp-Apim-Subscription-Key": os.getenv('SPEECH_KEY', '')
        }
        
        definition = {
            "locales": get_locales()
        }
        
        with open(filename, "rb") as audio_file:
            files = {
                "audio": audio_file,
                "definition": (None, json.dumps(definition), "application/json")
            }
            
            response = requests.post(url, headers=headers, files=files)
            response.raise_for_status()
            
            result: Dict[str, Any] = response.json()
            
            if not result.get("combinedPhrases"):
                return "No speech could be recognized."
                
            phrases = result["combinedPhrases"]
            if len(phrases) > 1:
                highest_confidence_phrase = max(
                    phrases, 
                    key=lambda x: x.get('confidence', 0)
                )
                logger.info(
                    f"Selected phrase in {highest_confidence_phrase.get('locale', 'unknown')} "
                    f"with confidence: {highest_confidence_phrase.get('confidence')}"
                )
                return highest_confidence_phrase['text']
            
            return phrases[0]['text']
            
    except requests.exceptions.HTTPError as e:
        error_msg = f"API Error: {e.response.status_code}, {e.response.text}"
        logger.error(error_msg)
        return f"Error: {error_msg}"
    except requests.exceptions.RequestException as e:
        error_msg = f"Network error: {str(e)}"
        logger.error(error_msg)
        return f"Error: {error_msg}"
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg)
        return f"Error: {error_msg}"