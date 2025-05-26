import mlflow.deployments
import os
from dotenv import load_dotenv

def call_databricks_endpoint(messages):
    """
    Call a Databricks endpoint with a list of messages.

    Args:
        messages (list): List of message dictionaries with 'role' and 'content' keys.

    Returns:
        dict: The response from the Databricks endpoint.

    Raises:
        Exception: If there is an error during the endpoint call.
    """
    load_dotenv()
    try:
        # Get the MLflow deployment client for Databricks
        client = mlflow.deployments.get_deploy_client("databricks")

        # Call the Databricks endpoint with the provided messages
        response = client.predict(
            endpoint=os.getenv("SERVING_ENDPOINT_NAME"),
            inputs={
                "messages": messages
            }
        )
        return response
    except Exception as e:
        # Log or print the error as needed
        print(f"Error calling Databricks endpoint: {e}")
        raise

if __name__ == "__main__":
    # Example usage for manual testing
    user_msg = input("Enter your message: ")
    messages = [{"role": "user", "content": user_msg}]
    try:
        response = call_databricks_endpoint(messages)
        print(response)
    except Exception as e:
        print(f"Failed to get response: {e}")

