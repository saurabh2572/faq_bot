import os
import logging
import requests
import uuid
import json
from typing import Any, Dict, List
from dotenv import load_dotenv

load_dotenv()

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AZURE_TRANSLATE_API_ENDPOINT = os.getenv("AZURE_TRANSLATE_API_ENDPOINT")
AZURE_TRANSLATE_API_KEY = os.getenv("AZURE_TRANSLATE_API_KEY")
AZURE_TRANSLATE_API_REGION = os.getenv("AZURE_TRANSLATE_API_REGION")

headers = {
    "Ocp-Apim-Subscription-Key": AZURE_TRANSLATE_API_KEY,
    "Ocp-Apim-Subscription-Region": AZURE_TRANSLATE_API_REGION,
    "Content-type": "application/json",
    "X-ClientTraceId": str(uuid.uuid4()),
}


def translate(
    text: str, target_languages: List[str], source_language: str = "en"
) -> str:
    """
    Translates the given text from the source language to the target language(s).

    Args:
        text (str): The text to be translated.
        target_languages (List[str]): List of target language codes.
        source_language (str): Source language code. Default is 'en' (English).

    Returns:
        str: The translated text in JSON format.
    """
    params = {
        "api-version": "3.0",
        "from": source_language,
        "to": ",".join(target_languages),
    }

    body = [{"text": text}]
    response = requests.post(
        AZURE_TRANSLATE_API_ENDPOINT, params=params, headers=headers, json=body
    )
    json_response = json.dumps(
        response.json(),
        sort_keys=True,
        ensure_ascii=False,
        indent=4,
        separators=(",", ": "),
    )
    return json_response


def translate_json(json_data: Dict[str, Any], target_language: str) -> Dict[str, Any]:
    """
    Translates the values in the provided JSON data into the specified target language.

    Args:
        json_data (dict): The JSON data to be translated.
        target_language (str): The target language code.

    Returns:
        dict: The translated JSON data.
    """
    translated_data = {}

    def traverse(obj: Any) -> Any:
        if isinstance(obj, dict):
            translated_obj = {}
            for key, value in obj.items():
                # translated_key = json.loads(translate(key, [target_language]))[0]['translations'][0]['text']
                translated_obj[key] = traverse(value)
            return translated_obj
        elif isinstance(obj, list):
            return [traverse(item) for item in obj]
        elif isinstance(obj, str):
            return json.loads(translate(obj, [target_language]))[0]["translations"][0][
                "text"
            ]
        else:
            return obj

    try:
        translated_data = traverse(json_data)
        return translated_data
    except Exception as e:
        logger.error(f"An error occurred during translation: {e}")
        raise
