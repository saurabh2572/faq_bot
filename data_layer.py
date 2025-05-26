"""
Custom Data Layer implementation for Chainlit chat application.

This module provides a custom implementation of Chainlit's BaseDataLayer
for storing and managing chat conversations, user feedback, and related data
in Azure Cosmos DB.

Classes:
    CustomDataLayer: Implements BaseDataLayer for Azure Cosmos DB storage
"""

import os
import chainlit as cl
import chainlit.data as cl_data
from typing import Dict, List, Optional
from datetime import datetime, timezone
from chainlit.types import (
    Feedback,
    PageInfo,
    PaginatedResponse,
    Pagination,
    ThreadDict,
    ThreadFilter,
)
from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import (
    CosmosResourceNotFoundError,
    CosmosHttpResponseError
)
from dotenv import load_dotenv
import logging
from utils import setup_logger
from cosmos_db import AzureCosmosClass

# Configure logging
logger = setup_logger("data_layer")

# Suppress Azure SDK logs
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.cosmos").setLevel(logging.WARNING)

# Load environment variables
load_dotenv()

# Cosmos DB Configuration
try:
    COSMOS_DB_ENDPOINT = os.getenv("COSMOS_DB_HOST")
    COSMOS_DB_KEY = os.getenv("COSMOS_DB_KEY")
    CHAINLIT_COSMOS_DB_NAME = os.getenv("CHAINLIT_COSMOS_DB_NAME")
    CHAINLIT_THREADS_CONTAINER = os.getenv("CHAINLIT_THREADS_CONTAINER")
    CHAINLIT_STEPS_CONTAINER = os.getenv("CHAINLIT_STEPS_CONTAINER")
    CHAINLIT_COSMOS_PARTITION_KEY = os.getenv("CHAINLIT_COSMOS_PARTITION_KEY")

    # Validate required environment variables
    if not all([
        COSMOS_DB_ENDPOINT,
        COSMOS_DB_KEY,
        CHAINLIT_COSMOS_DB_NAME,
        CHAINLIT_THREADS_CONTAINER,
        CHAINLIT_STEPS_CONTAINER,
        CHAINLIT_COSMOS_PARTITION_KEY
    ]):
        raise ValueError("Missing required environment variables")

except Exception as e:
    logger.error(f"Configuration error: {str(e)}")
    raise


class CustomDataLayer(cl_data.BaseDataLayer):
    """
    Custom implementation of Chainlit's BaseDataLayer for Azure Cosmos DB.

    This class handles storage and retrieval of chat conversations, user feedback,
    and related data using Azure Cosmos DB as the backend storage solution.

    Attributes:
        client: CosmosClient instance for database operations
        database: Cosmos DB database instance
        threads_container: Container for storing chat threads
        steps_container: Container for storing conversation steps
        conversations_cosmos: Instance of AzureCosmosClass for conversation management
    """

    def __init__(self):
        """
        Initialize the CustomDataLayer with Cosmos DB connections.

        Raises:
            CosmosHttpResponseError: If database/container creation fails
            Exception: For other initialization errors
        """
        try:
            logger.info("Initializing CustomDataLayer")
            self.client = CosmosClient(COSMOS_DB_ENDPOINT, COSMOS_DB_KEY)
            
            # Initialize database and containers
            self.database = self.client.create_database_if_not_exists(
                id=CHAINLIT_COSMOS_DB_NAME
            )
            self.threads_container = self.database.create_container_if_not_exists(
                id=CHAINLIT_THREADS_CONTAINER,
                partition_key=PartitionKey(path=CHAINLIT_COSMOS_PARTITION_KEY)
            )
            self.steps_container = self.database.create_container_if_not_exists(
                id=CHAINLIT_STEPS_CONTAINER,
                partition_key=PartitionKey(path=CHAINLIT_COSMOS_PARTITION_KEY)
            )
            
            # Initialize conversation handler
            self.conversations_cosmos = AzureCosmosClass()
            logger.info("CustomDataLayer initialized successfully")
            
        except CosmosHttpResponseError as e:
            logger.error(f"Cosmos DB initialization failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Initialization error: {str(e)}")
            raise

    async def upsert_feedback(self, feedback: Feedback) -> str:
        """
        Update or insert feedback for a conversation step.

        Args:
            feedback (Feedback): Feedback object containing value and comments

        Returns:
            str: The ID of the message that received feedback

        Raises:
            ValueError: If step or user message not found
            Exception: For other unexpected errors
        """
        try:
            logger.info(f"Processing feedback for step: {feedback.forId}")
            step_id = feedback.forId
            step = await self.get_step(step_id)
            
            if not step:
                raise ValueError(f"Step not found: {step_id}")
                
            user_message = self.find_user_message(step)
            if not user_message:
                raise ValueError(f"User message not found in step: {step_id}")
                
            await self.store_feedback(user_message, feedback.value, feedback.comment)
            logger.info(f"Feedback successfully processed for message: {user_message['id']}")
            return user_message['id']
            
        except Exception as e:
            logger.error(f"Failed to upsert feedback: {str(e)}")
            raise

    async def get_step(self, step_id: str) -> Optional[Dict]:
        """
        Retrieve a specific conversation step from Cosmos DB.

        Args:
            step_id (str): Unique identifier for the step

        Returns:
            Optional[Dict]: Step data if found, None otherwise

        Raises:
            CosmosHttpResponseError: If Cosmos DB query fails
        """
        try:
            query = f'SELECT * FROM Steps s WHERE s.id = "{step_id}"'
            items = list(self.steps_container.query_items(
                query=query,
                enable_cross_partition_query=True
            ))
            return items[0] if items else None
            
        except CosmosHttpResponseError as e:
            logger.error(f"Failed to query step {step_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error retrieving step: {str(e)}")
            raise

    def find_user_message(self, step: Dict) -> Dict:
        """
        Extract user message details from a conversation step.

        Args:
            step (Dict): Step data containing message information

        Returns:
            Dict: Formatted message data with input, thread_id, and id

        Raises:
            ValueError: If step data is invalid or incomplete
        """
        try:
            if not step:
                raise ValueError("Step data is empty")

            if step.get('name') == "on_audio_end":
                message = {
                    'input': step.get('input', ''),
                    'thread_id': step.get('threadId', ''),
                    'id': step.get('id', '')
                }
            elif step.get('name') == "on_message":
                message = {
                    'input': step.get('input', ''),
                    'thread_id': step.get('threadId', ''),
                    'id': step.get('parentId', '')
                }
            else:
                raise ValueError(f"Unknown step type: {step.get('name')}")

            # Validate required fields
            if not all([message['thread_id'], message['id']]):
                raise ValueError("Missing required message fields")

            return message

        except Exception as e:
            logger.error(f"Error processing user message: {str(e)}")
            raise

    async def store_feedback(
        self,
        message: Dict,
        value: int,
        comment: str
    ) -> None:
        """
        Store user feedback in both local storage and Cosmos DB.

        Args:
            message (Dict): Message details including ID and content
            value (int): Feedback rating value
            comment (str): User's feedback comments

        Raises:
            CosmosResourceNotFoundError: If thread not found
            CosmosHttpResponseError: If Cosmos DB operation fails
        """
        try:
            feedback_data = {
                'message_id': message['id'],
                'user_message': message['input'],
                'value': value,
                'comment': comment,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            thread_id = message['thread_id']

            # Store in local thread container
            try:
                thread = self.threads_container.read_item(
                    item=thread_id,
                    partition_key=thread_id
                )
            except CosmosResourceNotFoundError:
                logger.info(f"Creating new thread: {thread_id}")
                thread = {
                    'id': thread_id,
                    'feedback': []
                }

            # Update thread with feedback
            if 'feedback' not in thread:
                thread['feedback'] = []
            thread['feedback'].append(feedback_data)
            
            self.threads_container.upsert_item(thread)
            logger.info(f"Feedback stored locally for message: {message['id']}")

            # Store in Cosmos DB
            try:
                self.conversations_cosmos.upsert_feedback(
                    chat_id=thread_id,
                    message_id=message['id'],
                    feedback_vote=-1 if value == 0 else value,
                    feedback_text=comment if comment else ""
                )
                logger.info("Feedback successfully stored in Cosmos DB")
                
            except Exception as e:
                logger.error(f"Failed to store feedback in Cosmos DB: {str(e)}")
                # Continue execution since local storage succeeded

        except Exception as e:
            logger.error(f"Failed to store feedback: {str(e)}")
            raise

    async def get_user(self, identifier: str):
        print("get_user is called")
        pass

    async def create_user(self, user: cl.User):
        print("create_user is called")
        pass

    async def delete_feedback(self, feedback_id: str) -> bool:
        print("delete_feedback is called")
        query = f'SELECT * FROM Threads t WHERE ARRAY_CONTAINS(t.feedback, {{ "message_id": "{feedback_id}" }}, true)'
        items = list(self.threads_container.query_items(query=query, enable_cross_partition_query=True))
        if items:
            thread = items[0]
            thread['feedback'] = [fb for fb in thread['feedback'] if fb['message_id'] != feedback_id]
            self.threads_container.upsert_item(thread)
            chat_id = thread['id']
            msg_id = feedback_id

            # Call the feedback reset API
            try:
                api_feedback_data = {
                    'chat_id': chat_id,
                    'msg_id': msg_id
                }
                # marked
                self.conversations_cosmos.reset_feedback(
                    chat_id = api_feedback_data['chat_id'],
                    message_id = api_feedback_data['msg_id']
                    )
                # response = requests.delete(f"{FEEDBACK_API}/reset", json=api_feedback_data)
                # response.raise_for_status()
                print("Feedback reset request sent to cosmos db successfully")
                
            # except requests.exceptions.RequestException as e:
            #     print(f"Failed to send feedback reset request to API: {e}")
            
            except Exception as e:
                print(f"Failed to send feedback reset request to Cosmos DB: {e}")
                # Don't raise exception here since local deletion was successful

            return True
        return False

    @cl_data.queue_until_user_message()
    async def create_element(self, element: "Element"):
        print("create_element is called")
        pass

    async def get_element(self, thread_id: str, element_id: str) -> Optional["ElementDict"]:
        print("get_element is called")
        pass

    @cl_data.queue_until_user_message()
    async def delete_element(self, element_id: str, thread_id: Optional[str] = None):
        print("delete_element is called")
        pass

    async def create_step(self, step_dict: "StepDict") -> None:
        """
        Create a new conversation step in Cosmos DB.

        Args:
            step_dict (StepDict): Dictionary containing step information

        Raises:
            CosmosHttpResponseError: If step creation fails
        """
        try:
            logger.info(f"Creating step: {step_dict.get('id')}")
            self.steps_container.upsert_item(step_dict)
            logger.info(f"Step created successfully: {step_dict.get('id')}")
            
        except CosmosHttpResponseError as e:
            logger.error(f"Failed to create step: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating step: {str(e)}")
            raise

    async def update_step(self, step_dict: "StepDict") -> None:
        """
        Update an existing conversation step.

        Args:
            step_dict (StepDict): Updated step information

        Raises:
            CosmosHttpResponseError: If step update fails
        """
        try:
            step_id = step_dict.get('id')
            logger.info(f"Updating step: {step_id}")
            self.steps_container.upsert_item(step_dict)
            logger.info(f"Step updated successfully: {step_id}")
            
        except CosmosHttpResponseError as e:
            logger.error(f"Failed to update step: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error updating step: {str(e)}")
            raise

    async def delete_step(self, step_id: str) -> None:
        """
        Delete a conversation step from Cosmos DB.

        Args:
            step_id (str): Unique identifier of the step to delete

        Raises:
            CosmosResourceNotFoundError: If step doesn't exist
            CosmosHttpResponseError: If deletion fails
        """
        try:
            logger.info(f"Deleting step: {step_id}")
            self.steps_container.delete_item(
                item=step_id,
                partition_key=step_id
            )
            logger.info(f"Step deleted successfully: {step_id}")
            
        except CosmosResourceNotFoundError as e:
            logger.warning(f"Step not found: {step_id}")
            raise
        except CosmosHttpResponseError as e:
            logger.error(f"Failed to delete step: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error deleting step: {str(e)}")
            raise

    async def get_thread_author(self, thread_id: str) -> str:
        """
        Retrieve the author of a thread.

        Args:
            thread_id (str): Unique identifier of the thread

        Returns:
            str: Author identifier

        Raises:
            NotImplementedError: Method not implemented yet
        """
        logger.warning("get_thread_author not implemented")
        raise NotImplementedError("Thread author retrieval not implemented")

    async def delete_thread(self, thread_id: str) -> None:
        """
        Delete a thread and all its associated data.

        Args:
            thread_id (str): Unique identifier of the thread to delete

        Raises:
            CosmosResourceNotFoundError: If thread doesn't exist
            CosmosHttpResponseError: If deletion fails
        """
        try:
            logger.info(f"Deleting thread: {thread_id}")
            
            # Delete thread document
            self.threads_container.delete_item(
                item=thread_id,
                partition_key=thread_id
            )
            
            # Delete associated steps
            query = f'SELECT * FROM Steps s WHERE s.threadId = "{thread_id}"'
            steps = list(self.steps_container.query_items(
                query=query,
                enable_cross_partition_query=True
            ))
            
            for step in steps:
                self.steps_container.delete_item(
                    item=step['id'],
                    partition_key=step['id']
                )
                
            logger.info(f"Thread and associated data deleted: {thread_id}")
            
        except CosmosResourceNotFoundError as e:
            logger.warning(f"Thread not found: {thread_id}")
            raise
        except CosmosHttpResponseError as e:
            logger.error(f"Failed to delete thread: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error deleting thread: {str(e)}")
            raise

    async def list_threads(
        self,
        pagination: Pagination,
        filters: ThreadFilter
    ) -> PaginatedResponse[ThreadDict]:
        """
        Retrieve a paginated list of threads based on filters.

        Args:
            pagination (Pagination): Pagination parameters
            filters (ThreadFilter): Filter criteria for threads

        Returns:
            PaginatedResponse[ThreadDict]: Paginated thread results

        Raises:
            CosmosHttpResponseError: If query execution fails
        """
        try:
            logger.info("Retrieving thread list with filters")
            
            # Build query with filters
            query = ["SELECT * FROM Threads t WHERE 1=1"]
            
            if filters.user_id:
                query.append(f"AND t.userId = '{filters.user_id}'")
            if filters.tag:
                query.append(f"AND ARRAY_CONTAINS(t.tags, '{filters.tag}')")
            
            # Add pagination
            offset = (pagination.page - 1) * pagination.page_size
            query.append(f"OFFSET {offset} LIMIT {pagination.page_size}")
            
            # Execute query
            items = list(self.threads_container.query_items(
                query=" ".join(query),
                enable_cross_partition_query=True
            ))
            
            # Get total count for pagination
            count_query = ["SELECT VALUE COUNT(1) FROM Threads t WHERE 1=1"]
            if filters.user_id:
                count_query.append(f"AND t.userId = '{filters.user_id}'")
            if filters.tag:
                count_query.append(f"AND ARRAY_CONTAINS(t.tags, '{filters.tag}')")
            
            total_count = list(self.threads_container.query_items(
                query=" ".join(count_query),
                enable_cross_partition_query=True
            ))[0]
            
            # Calculate pagination info
            total_pages = (total_count + pagination.page_size - 1) // pagination.page_size
            has_next = pagination.page < total_pages
            has_previous = pagination.page > 1
            
            page_info = PageInfo(
                current_page=pagination.page,
                total_pages=total_pages,
                has_next=has_next,
                has_previous=has_previous
            )
            
            logger.info(f"Retrieved {len(items)} threads")
            return PaginatedResponse(data=items, page_info=page_info)
            
        except CosmosHttpResponseError as e:
            logger.error(f"Failed to query threads: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in list_threads: {str(e)}")
            raise

    async def get_thread(self, thread_id: str) -> Optional[ThreadDict]:
        """
        Retrieve a specific thread by ID.

        Args:
            thread_id (str): Unique identifier of the thread

        Returns:
            Optional[ThreadDict]: Thread data if found, None otherwise

        Raises:
            CosmosHttpResponseError: If thread retrieval fails
        """
        try:
            logger.info(f"Retrieving thread: {thread_id}")
            thread = self.threads_container.read_item(
                item=thread_id,
                partition_key=thread_id
            )
            return thread
            
        except CosmosResourceNotFoundError:
            logger.warning(f"Thread not found: {thread_id}")
            return None
        except CosmosHttpResponseError as e:
            logger.error(f"Failed to retrieve thread: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error retrieving thread: {str(e)}")
            raise

    async def update_thread(
        self,
        thread_id: str,
        name: Optional[str] = None,
        user_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
        tags: Optional[List[str]] = None
    ) -> None:
        """
        Update thread properties.

        Args:
            thread_id (str): Thread identifier
            name (Optional[str]): New thread name
            user_id (Optional[str]): New user identifier
            metadata (Optional[Dict]): Updated metadata
            tags (Optional[List[str]]): Updated tags list

        Raises:
            ValueError: If thread doesn't exist
            CosmosHttpResponseError: If update fails
        """

        logger.info(f"Update thread called for thread id: {thread_id}. But its not implemented.")
        pass
        # try:
        #     logger.info(f"Updating thread: {thread_id}")
            
        #     # Get existing thread
        #     thread = await self.get_thread(thread_id)
        #     if not thread:
        #         raise ValueError(f"Thread not found: {thread_id}")

        #     # Update fields if provided
        #     if name is not None:
        #         thread['name'] = name
        #     if user_id is not None:
        #         thread['userId'] = user_id
        #     if metadata is not None:
        #         thread['metadata'] = metadata
        #     if tags is not None:
        #         thread['tags'] = tags

        #     # Save updates
        #     self.threads_container.replace_item(
        #         item=thread_id,
        #         body=thread
        #     )
        #     logger.info(f"Thread updated successfully: {thread_id}")
            
        # except CosmosHttpResponseError as e:
        #     logger.error(f"Failed to update thread: {str(e)}")
        #     raise
        # except Exception as e:
        #     logger.error(f"Unexpected error updating thread: {str(e)}")
        #     raise

    async def build_debug_url(self) -> str:
        """
        Generate debug URL for troubleshooting.

        Returns:
            str: Debug URL

        Note:
            This is a placeholder method that needs implementation
        """
        logger.warning("build_debug_url not implemented")
        return "Debug URL not implemented"