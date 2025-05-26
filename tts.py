import os
from dotenv import load_dotenv
import azure.cognitiveservices.speech as speechsdk

load_dotenv()

# """
#   For more samples please visit https://github.com/Azure-Samples/cognitive-services-speech-sdk
# """
# Creates an instance of a speech config with specified subscription key and service region.
SPEECH_API_KEY = os.getenv("SPEECH_API_KEY")
SPEECH_API_SERVICE_REGION = os.getenv("SPEECH_API_SERVICE_REGION")
file_name = "outputaudio.wav"
file_config = speechsdk.audio.AudioOutputConfig(filename=file_name)
speech_config = speechsdk.SpeechConfig(
    subscription=SPEECH_API_KEY, region=SPEECH_API_SERVICE_REGION
)

# speech_synthesizer = speechsdk.SpeechSynthesizer(
#     speech_config=speech_config, audio_config=file_config
# )
speech_config.speech_synthesis_voice_name = "en-US-AvaMultilingualNeural"


async def text_to_speech(text):
    # use the default speaker as audio output.
    speech_synthesizer = speechsdk.SpeechSynthesizer(
        speech_config=speech_config, audio_config=file_config
    )
    result = speech_synthesizer.speak_text_async(text).get()
    # Check result
    if result.reason == speechsdk.ResultReason.SynthesizingAudioCompleted:
        print("Speech synthesized for text [{}]".format(text))
    elif result.reason == speechsdk.ResultReason.Canceled:
        cancellation_details = result.cancellation_details
        print("Speech synthesis canceled: {}".format(cancellation_details.reason))
        if cancellation_details.reason == speechsdk.CancellationReason.Error:
            print("Error details: {}".format(cancellation_details.error_details))
