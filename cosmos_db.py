import os
import json
from typing import List, Dict, Union, Optional
from dotenv import load_dotenv
from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import CosmosHttpResponseError
import logging
import time
import uuid
from datetime import datetime, timezone
from utils import setup_logger

# Configure logging
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.cosmos").setLevel(logging.WARNING)
logger = setup_logger("cosmos_db")


class AzureCosmosClass:
    """
    A class to handle Azure Cosmos DB operations for chat conversations.

    This class provides methods to:
    - Initialize connection to Cosmos DB
    - Create and manage chat conversations
    - Update conversation history
    - Handle user feedback
    
    Attributes:
        COSMOS_HOST (str): The host URL for Cosmos DB
        COSMOS_MASTER_KEY (str): Authentication key for Cosmos DB
        DATABASE_ID (str): The database identifier
        CONTAINER_ID (str): The container identifier
        partition_key (str): Key used for data partitioning
    """

    def __init__(self) -> None:
        """
        Initialize Cosmos DB connection and container.
        
        Raises:
            ValueError: If required environment variables are missing
            CosmosHttpResponseError: If database/container creation fails
        """
        try:
            load_dotenv()
            
            # Validate required environment variables
            required_vars = [
                'COSMOS_DB_HOST', 'COSMOS_DB_KEY', 'CONVERSATIONS_DB',
                'CONVERSATIONS_PARTITION_KEY', 'CONVERSATIONS_CONTAINER'
            ]
            
            missing_vars = [var for var in required_vars 
                          if not os.getenv(var)]
            
            if missing_vars:
                raise ValueError(
                    f"Missing required environment variables: {', '.join(missing_vars)}"
                )

            self.COSMOS_HOST = os.getenv('COSMOS_DB_HOST')
            self.COSMOS_MASTER_KEY = os.getenv('COSMOS_DB_KEY')
            self.DATABASE_ID = os.getenv('CONVERSATIONS_DB')
            self.partition_key = os.getenv('CONVERSATIONS_PARTITION_KEY')
            self.CONTAINER_ID = os.getenv('CONVERSATIONS_CONTAINER')

            # Initialize Cosmos DB client
            self.client = CosmosClient(self.COSMOS_HOST, self.COSMOS_MASTER_KEY)
            self.database_object = self.client.create_database_if_not_exists(
                id=self.DATABASE_ID
            )
            self.container_object = self.database_object.create_container_if_not_exists(
                id=self.CONTAINER_ID,
                partition_key=PartitionKey(path=f"/{self.partition_key}")
            )
            
        except CosmosHttpResponseError as e:
            logger.error(f"Cosmos DB initialization failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Initialization error: {str(e)}")
            raise

    def upload_data(self, chat_id: str) -> None:
        """
        Create a new conversation entry in Cosmos DB.

        Args:
            chat_id (str): Unique identifier for the conversation

        Raises:
            CosmosHttpResponseError: If creation of container item fails
        """
        try:
            conversation_data = {
                "id": chat_id,
                self.partition_key: f"{chat_id}_partkey",
                "conversation": []
            }
            self.container_object.create_item(body=conversation_data)
            logger.info(f"Created new conversation with chat_id: {chat_id}")
            
        except CosmosHttpResponseError as e:
            logger.error(f"Cosmos DB operation failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Failed to create conversation: {str(e)}")
            raise

    def get_chat_history(self, chat_id: str) -> List[Dict[str, str]]:
        """
        Retrieve conversation history for a given chat ID.

        Args:
            chat_id (str): The conversation identifier

        Returns:
            List[Dict[str, str]]: List of conversation messages

        Raises:
            Exception: If retrieval of chat history fails
        """
        try:
            existing_data = self.get_data(chat_id)
            
            if not existing_data:
                self.upload_data(chat_id)
                return []
                
            conversation = existing_data.get('conversation', [])
            chat_history = []
            
            for msg in conversation:
                if msg.get('user_message'):
                    chat_history.append({
                        "role": "user",
                        "content": msg['user_message']
                    })
                if msg.get('ai_answer'):
                    chat_history.append({
                        "role": "assistant",
                        "content": msg['ai_answer']
                    })
                    
            return chat_history
            
        except Exception as e:
            logger.error(f"Failed to retrieve chat history: {str(e)}")
            raise

    def update_conversation(
            self,
            databricks_request_id: str,
            chat_id: str,
            message_id: str,
            user_message: str,
            rephrased_message: str,
            check_query: str,
            ai_answer: str,
            context: str,
            comparison_details: Optional[Dict]
    ) -> None:
        """
        Updates an existing conversation with new message details.

        Args:
            databricks_request_id (str): Request ID from Databricks
            chat_id (str): Conversation identifier
            message_id (str): Unique message identifier
            user_message (str): Original user message
            rephrased_message (str): Processed/rephrased user message
            check_query (str): Query validation string
            ai_answer (str): AI-generated response
            context (str): Knowledge base context used
            comparison_details (Dict): Message comparison metadata

        Raises:
            CosmosHttpResponseError: If Cosmos DB operation fails
            ValueError: If required conversation is not found
            Exception: For other unexpected errors
        """
        try:
            partition_key = f"{chat_id}_partkey"
            prev_item = self.container_object.read_item(
                item=chat_id,
                partition_key=partition_key
            )

            new_message = {
                "databricks_request_id": databricks_request_id,
                "message_id": message_id,
                "user_message": user_message,
                "rephrased_message": rephrased_message,
                "check_query": check_query,
                "comparison_details": comparison_details,
                "ai_answer": ai_answer,
                "context": context,
                "feedback_vote": 0,
                "feedback_text": "",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            prev_item['conversation'].append(new_message)
            self.container_object.replace_item(
                item=chat_id,
                body=prev_item
            )
            logger.info(f"Successfully updated conversation for chat_id: {chat_id}")

        except CosmosHttpResponseError as e:
            logger.error(f"Cosmos DB operation failed for chat_id {chat_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Failed to update conversation for chat_id {chat_id}: {str(e)}")
            raise

    def get_data(self, conversation_id: str) -> Union[Dict, bool]:
        """
        Retrieve conversation data from Cosmos DB.

        Args:
            conversation_id (str): Unique identifier for the conversation

        Returns:
            Union[Dict, bool]: Conversation data if found, False otherwise

        Raises:
            CosmosHttpResponseError: If Cosmos DB read operation fails
        """
        partition_key = f"{conversation_id}_partkey"
        
        try:
            item = self.container_object.read_item(
                item=conversation_id,
                partition_key=partition_key
            )
            return item
        except CosmosHttpResponseError:
            logger.info(f"No existing conversation found for ID: {conversation_id}")
            return False
        except Exception as e:
            logger.error(f"Error retrieving conversation data: {str(e)}")
            raise

    def upsert_feedback(
            self,
            chat_id: str,
            message_id: str,
            feedback_vote: str,
            feedback_text: str
    ) -> None:
        """
        Update or insert feedback for a specific message.

        Args:
            chat_id (str): Conversation identifier
            message_id (str): Message identifier
            feedback_vote (str): User's feedback rating
            feedback_text (str): User's feedback comments

        Raises:
            ValueError: If message_id is not found in conversation
            CosmosHttpResponseError: If Cosmos DB operation fails
            Exception: For other unexpected errors
        """
        try:
            partition_key = f"{chat_id}_partkey"
            item = self.container_object.read_item(
                item=chat_id,
                partition_key=partition_key
            )

            message_found = False
            for message in item['conversation']:
                if message['message_id'] == message_id:
                    message['feedback_vote'] = feedback_vote
                    message['feedback_text'] = feedback_text
                    message_found = True
                    break

            if not message_found:
                raise ValueError(f"Message ID {message_id} not found in conversation")

            self.container_object.replace_item(item=chat_id, body=item)
            logger.info(
                f"Feedback updated for message {message_id} in chat {chat_id}"
            )

        except CosmosHttpResponseError as e:
            logger.error(f"Cosmos DB feedback update failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Failed to update feedback: {str(e)}")
            raise

    def reset_feedback(self, chat_id: str, message_id: str) -> None:
        """
        Reset feedback values for a specific message.

        Args:
            chat_id (str): Conversation identifier
            message_id (str): Message identifier

        Raises:
            ValueError: If message_id is not found
            CosmosHttpResponseError: If Cosmos DB operation fails
            Exception: For other unexpected errors
        """
        try:
            partition_key = f"{chat_id}_partkey"
            item = self.container_object.read_item(
                item=chat_id,
                partition_key=partition_key
            )

            message_found = False
            for message in item['conversation']:
                if message['message_id'] == message_id:
                    message['feedback_vote'] = 0
                    message['feedback_text'] = ""
                    message_found = True
                    break

            if not message_found:
                raise ValueError(f"Message ID {message_id} not found in conversation")

            self.container_object.replace_item(item=chat_id, body=item)
            logger.info(
                f"Feedback reset for message {message_id} in chat {chat_id}"
            )

        except CosmosHttpResponseError as e:
            logger.error(f"Cosmos DB feedback reset failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Failed to reset feedback: {str(e)}")
            raise