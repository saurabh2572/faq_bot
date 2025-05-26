"""
Chainlit-based Chat Application with Speech Recognition Support.

This module implements a chat interface with the following features:
- Text-based conversation with AI
- Speech-to-text conversion
- Azure Cosmos DB integration for conversation storage
- Custom error handling and logging

The application uses environment variables for configuration and
supports both text and voice inputs for user interaction.
"""

import os
import json
import io
import uuid
import wave
import logging
from typing import Optional, Dict, Any

import numpy as np
import httpx
import chainlit as cl
import chainlit.data as cl_data
from chainlit.types import ThreadDict
from chainlit.input_widget import Select
from dotenv import load_dotenv

from speech_recognition import recognize_from_file
from utils import delete_audio_file, setup_logger
from data_layer import CustomDataLayer
from cosmos_db import AzureCosmosClass
from databricks_utils import call_databricks_endpoint

# Configure logging
logger = setup_logger("app")

# Suppress third-party library logs
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.cosmos").setLevel(logging.WARNING)

# Load and validate environment variables
load_dotenv()

required_env_vars = [
    "CHATBOT_NAME",
    "WELCOME_MESSAGE",
    "LANGUAGE"
]

missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

CHATBOT_NAME = os.getenv("CHATBOT_NAME")
WELCOME_MESSAGE = os.getenv("WELCOME_MESSAGE")
LANGUAGE = os.getenv("LANGUAGE")

# Initialize custom data layer
cl_data._data_layer = CustomDataLayer()


@cl.step(name="Answer generator...", type="tool")
async def get_response(chat_id: str, msg_id: str, query: str):
    """
    Generate AI response for user query using Databricks endpoint.

    Args:
        chat_id (str): Unique identifier for the chat session
        msg_id (str): Unique identifier for the message
        query (str): User's input message

    Returns:
        Dict[str, Any]: AI response or error message

    Raises:
        Exception: For any errors during response generation
    """
    try:
        # Initialize Cosmos DB client and get chat history
        conversations_cosmos_client = AzureCosmosClass()
        chat_history = conversations_cosmos_client.get_chat_history(chat_id=chat_id)
        chat_history.append({"role": "user", "content": query})

        # Call Databricks endpoint for response
        response = call_databricks_endpoint(messages=chat_history)
        if not response:
            raise ValueError("Empty response from Databricks endpoint")

        answer = response['messages'][0]['content']
        custom_outputs = response.get("custom_outputs", {})
        databricks_request_id = response.get("databricks_output", {}).get(
            "databricks_request_id"
        )

        # Update conversation in Cosmos DB
        conversations_cosmos_client.update_conversation(
            databricks_request_id=databricks_request_id,
            chat_id=chat_id,
            message_id=msg_id,
            user_message=query,
            rephrased_message=custom_outputs.get("rephrased_query", ""),
            check_query=custom_outputs.get("check_query", ""),
            ai_answer=answer,
            context=custom_outputs["context"],
            comparison_details=custom_outputs.get("comparison_details", None)
        )
        
        return answer

    except Exception as e:
        logger.error(f"Error in get_response: {str(e)}", exc_info=True)
        return {
            "error": "Sorry, something went wrong. Please try again later.",
            "details": str(e)
        }


@cl.on_chat_start
async def on_chat_start():
    """Send a welcome message when the chat starts."""
    try:
        await cl.Message(content=WELCOME_MESSAGE, author=CHATBOT_NAME).send()
    except Exception as e:
        logger.error(f"Error in on_chat_start: {e}")


@cl.on_message
async def on_message(msg: cl.Message) -> None:
    """
    Handle incoming chat messages and generate responses.

    Args:
        msg (cl.Message): The incoming message object containing user input

    Note:
        Processes the message and generates AI response using get_response()
    """
    try:
        msg_id = msg.id
        chat_id = msg.thread_id

        cl.user_session.set("thread_id", chat_id)
        logger.info(f"Processing message: msg_id={msg_id}, chat_id={chat_id}")

        response = await get_response(
            chat_id=chat_id,
            msg_id=msg_id,
            query=msg.content
        )
        
        if isinstance(response, dict) and "error" in response:
            await cl.Message(
                content="I apologize, but I encountered an error. Please try again.",
                author=CHATBOT_NAME
            ).send()
            return

        await cl.Message(content=response, author=CHATBOT_NAME).send()
        logger.info(f"Response sent for message: {msg_id}")

    except Exception as e:
        logger.error(f"Error processing message: {str(e)}", exc_info=True)
        await cl.Message(
            content="An error occurred while processing your message. Please try again."
        ).send()


@cl.step(name="Speech to text...", type="tool")
async def speech_to_text(audio_file: str) -> str:
    """
    Convert speech from an audio file to text.

    Args:
        audio_file (str): Path to the audio file

    Returns:
        str: Transcribed text or error message

    Raises:
        Exception: If speech recognition fails
    """
    try:
        logger.info(f"Starting speech recognition for file: {audio_file}")
        transcription = recognize_from_file(filename=audio_file)
        
        if not transcription:
            raise ValueError("No transcription generated")
            
        logger.info("Speech recognition completed successfully")
        return transcription

    except Exception as e:
        logger.error(f"Speech recognition failed: {str(e)}", exc_info=True)
        return "I couldn't understand the audio. Please try again."


@cl.on_audio_start
async def on_audio_start() -> bool:
    """
    Initialize session storage for audio recording.

    Returns:
        bool: True if initialization successful, False otherwise
    """
    try:
        cl.user_session.set("audio_chunks", [])
        logger.info("Audio recording session initialized")
        return True

    except Exception as e:
        logger.error(f"Audio session initialization failed: {str(e)}", exc_info=True)
        return False


@cl.on_audio_chunk
async def on_audio_chunk(chunk: cl.InputAudioChunk) -> None:
    """
    Collect and process incoming audio chunks during recording.

    Args:
        chunk (cl.InputAudioChunk): Raw audio data chunk
    """
    try:
        audio_chunks = cl.user_session.get("audio_chunks")
        if audio_chunks is None:
            raise ValueError("Audio chunks storage not initialized")

        audio_chunk = np.frombuffer(chunk.data, dtype=np.int16)
        audio_chunks.append(audio_chunk)

    except Exception as e:
        logger.error(f"Error processing audio chunk: {str(e)}", exc_info=True)


@cl.on_audio_end
async def on_audio_end():
    """Process the recorded audio after recording stops."""
    try:
        await process_audio()
    except Exception as e:
        logger.error(f"Error in on_audio_end: {e}")


async def process_audio() -> None:
    """
    Process recorded audio: save, transcribe, and generate response.
    
    Handles the complete workflow of:
    1. Concatenating audio chunks
    2. Saving to WAV file
    3. Transcribing to text
    4. Generating AI response
    5. Cleaning up temporary files
    """
    try:
        audio_chunks = cl.user_session.get("audio_chunks")
        if not audio_chunks:
            await cl.Message(
                content="No audio recorded. Please try again."
            ).send()
            return

        # Generate unique audio file path
        audio_id = str(uuid.uuid4())
        audio_file_path = f"temp_{audio_id}_recorded_audio.wav"
        
        try:
            # Save audio to WAV file
            concatenated = np.concatenate(audio_chunks)
            with wave.open(audio_file_path, 'wb') as wav_file:
                wav_file.setnchannels(1)
                wav_file.setsampwidth(2)
                wav_file.setframerate(24000)
                wav_file.writeframes(concatenated.tobytes())

            # Clear stored audio chunks
            cl.user_session.set("audio_chunks", [])

            # Process speech to text
            logger.info("Starting speech-to-text conversion")
            transcription = await speech_to_text(audio_file_path)

            if not transcription or transcription.startswith("I couldn't understand"):
                raise ValueError("Speech recognition failed")

            # Send transcription message
            message_transcription = cl.Message(
                author="You",
                type="user_message",
                content=transcription
            )
            await message_transcription.send()

            # Generate AI response
            answer = await get_response(
                chat_id=message_transcription.thread_id,
                msg_id=message_transcription.parent_id,
                query=transcription
            )
            cl.user_session.set("thread_id", message_transcription.thread_id)
            await cl.Message(content=answer).send()
            logger.info("Audio processing completed successfully")

        finally:
            # Cleanup temporary file
            if os.path.exists(audio_file_path):
                delete_audio_file(audio_file_path=audio_file_path)

    except Exception as e:
        logger.error(f"Audio processing failed: {str(e)}", exc_info=True)
        await cl.Message(
            content="Sorry, I encountered an error processing the audio. Please try again."
        ).send()


@cl.on_stop
async def on_stop() -> None:
    """
    Handle task interruption by user.

    Triggered when the user explicitly requests to stop the current operation.
    Performs cleanup and logs the interruption.
    """
    try:
        logger.info("Task interruption requested by user")
        # Clean up any ongoing operations
        audio_chunks = cl.user_session.get("audio_chunks")
        if audio_chunks:
            cl.user_session.set("audio_chunks", [])
            logger.info("Cleaned up audio session data")

        await cl.Message(
            content="Task stopped as requested."
        ).send()

    except Exception as e:
        logger.error(f"Error handling stop request: {str(e)}", exc_info=True)


@cl.on_chat_end
async def on_chat_end() -> None:
    """
    Handle chat session termination.

    Performs cleanup operations when user disconnects:
    - Cleans up session data
    - Logs session end
    - Releases any held resources
    """
    try:
        session_id = cl.user_session.get("id")
        logger.info(f"Chat session ended: {session_id}")

        thread_id = cl.user_session.get("thread_id")
        logger.info(f"Cleaning up session data for thread: {thread_id}")
        # Clear session data
        if thread_id:
            # call delete_thread from data_layer
            await cl_data._data_layer.delete_thread(thread_id=thread_id)
            logger.info(f"Deleted thread and steps data for thread: {thread_id}")

        # Clean up session resources
        if hasattr(cl.user_session, "audio_chunks"):
            cl.user_session.set("audio_chunks", None)

        # Log session statistics if available
        if hasattr(cl.user_session, "message_count"):
            msg_count = cl.user_session.get("message_count", 0)
            logger.info(f"Session {session_id} processed {msg_count} messages")

    except Exception as e:
        logger.error(f"Error during chat end cleanup: {str(e)}", exc_info=True)


@cl.on_chat_resume
async def on_chat_resume(thread: ThreadDict) -> None:
    """
    Handle chat session resumption.

    Args:
        thread (ThreadDict): Dictionary containing previous chat thread information

    Restores chat context and sends welcome back message to user.
    """
    try:
        thread_id = thread.get("id", "unknown")
        logger.info(f"Resuming chat session: {thread_id}")

        # Initialize session data
        cl.user_session.set("thread_id", thread_id)
        cl.user_session.set("message_count", len(thread.get("messages", [])))

        # Send welcome back message
        await cl.Message(
            content=f"Welcome back! Continuing your previous conversation.",
            author=CHATBOT_NAME
        ).send()

        logger.info(f"Successfully resumed session: {thread_id}")

    except Exception as e:
        logger.error(f"Error resuming chat session: {str(e)}", exc_info=True)
        await cl.Message(
            content="There was an error resuming your previous session. Starting a new conversation."
        ).send()


@cl.on_settings_update
async def setup_agent(settings: Dict[str, Any]) -> None:
    """
    Update agent settings based on user preferences.

    Args:
        settings (Dict[str, Any]): Dictionary containing updated settings

    Handles runtime configuration changes and validates new settings.
    """
    try:
        logger.info("Updating agent settings")
        
        # Validate settings
        if not isinstance(settings, dict):
            raise ValueError("Invalid settings format")

        # Apply language settings if present
        if "language" in settings:
            cl.user_session.set("language", settings["language"])
            logger.info(f"Language updated to: {settings['language']}")

        # Apply other custom settings
        for key, value in settings.items():
            if key != "language":
                cl.user_session.set(f"setting_{key}", value)
                logger.info(f"Updated setting {key}: {value}")

        await cl.Message(
            content="Settings updated successfully.",
            author=CHATBOT_NAME
        ).send()

    except Exception as e:
        logger.error(f"Error updating settings: {str(e)}", exc_info=True)
        await cl.Message(
            content="Failed to update settings. Please try again."
        ).send()


def cleanup_resources() -> None:
    """
    Perform cleanup of application resources.

    Should be called when shutting down the application or in error scenarios.
    Ensures proper cleanup of files, connections, and session data.
    """
    try:
        logger.info("Starting resource cleanup")
        
        # Clean up temporary audio files
        temp_dir = "."
        for file in os.listdir(temp_dir):
            if file.endswith("_recorded_audio.wav"):
                try:
                    os.remove(os.path.join(temp_dir, file))
                    logger.info(f"Removed temporary file: {file}")
                except OSError as e:
                    logger.warning(f"Failed to remove file {file}: {str(e)}")

        # Close any open connections
        # Add any additional cleanup needed

        logger.info("Resource cleanup completed")

    except Exception as e:
        logger.error(f"Error during resource cleanup: {str(e)}", exc_info=True)






