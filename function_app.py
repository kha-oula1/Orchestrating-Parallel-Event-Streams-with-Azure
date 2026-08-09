import json
import os
import uuid
from datetime import datetime, timezone

import azure.functions as func
import azure.durable_functions as df
from azure.cosmos import CosmosClient, exceptions


app = df.DFApp()


def _get_container(container_name):
    connection_string = os.environ["CosmosDBConnection"]
    client = CosmosClient.from_connection_string(connection_string)
    database = client.get_database_client("StreamData")
    return database.get_container_client(container_name)


@app.queue_trigger(
    arg_name="msg",
    queue_name="alerts",
    connection="AzureWebJobsStorage",
)
@app.durable_client_input(client_name="client")
async def alerts_queue(msg: func.QueueMessage, client):
    event = json.loads(msg.get_body().decode("utf-8"))
    await client.start_new("event_orchestrator", None, event)


@app.route(route="event", methods=["POST"])
@app.durable_client_input(client_name="client")
async def event_http(req: func.HttpRequest, client):
    event = req.get_json()
    instance_id = await client.start_new("event_orchestrator", None, event)
    return client.create_check_status_response(req, instance_id)


@app.route(route="stats/{stream_id}", methods=["GET"])
def stats_http(req: func.HttpRequest) -> func.HttpResponse:
    stream_id = req.route_params["stream_id"]
    container = _get_container("stats")

    try:
        stats = container.read_item(item=stream_id, partition_key=stream_id)
    except exceptions.CosmosResourceNotFoundError:
        stats = {
            "stream_id": stream_id,
            "follower_count": 0,
            "sub_count": 0,
            "total_donations": 0,
        }
    else:
        stats = {
            "stream_id": stats.get("stream_id", stream_id),
            "follower_count": stats.get("follower_count", 0),
            "sub_count": stats.get("sub_count", 0),
            "total_donations": stats.get("total_donations", 0),
        }

    return func.HttpResponse(
        body=json.dumps(stats),
        status_code=200,
        mimetype="application/json",
    )


@app.orchestration_trigger(context_name="context")
def event_orchestrator(context):
    event = context.get_input()
    results = yield context.task_all(
        [
            context.call_activity("update_stats", event),
            context.call_activity("send_alert", event),
            context.call_activity("log_analytics", event),
        ]
    )
    return results


@app.activity_trigger(input_name="event")
def update_stats(event):
    stream_id = event["stream_id"]
    container = _get_container("stats")

    try:
        stats = container.read_item(item=stream_id, partition_key=stream_id)
    except exceptions.CosmosResourceNotFoundError:
        stats = {
            "id": stream_id,
            "stream_id": stream_id,
            "follower_count": 0,
            "sub_count": 0,
            "total_donations": 0,
        }

    if event["type"] == "follower":
        stats["follower_count"] += 1
    elif event["type"] == "subscription":
        stats["sub_count"] += 1
    elif event["type"] == "donation":
        stats["total_donations"] += event.get("amount", 0)

    container.upsert_item(stats)
    return {"status": "updated"}


@app.activity_trigger(input_name="event")
def send_alert(event):
    alert = f"New {event['type']} from {event['username']}!"
    return {"status": "sent", "alert": alert}


@app.activity_trigger(input_name="event")
def log_analytics(event):
    document = {
        "id": str(uuid.uuid4()),
        "stream_id": event["stream_id"],
        "type": event["type"],
        "username": event["username"],
        "amount": event.get("amount"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    container = _get_container("analytics")
    container.create_item(document)
    return {"status": "logged", "document_id": document["id"]}
