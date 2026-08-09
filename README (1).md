<img src="https://cdn.prod.website-files.com/677c400686e724409a5a7409/6790ad949cf622dc8dcd9fe4_nextwork-logo-leather.svg" alt="NextWork" width="300" />

# Orchestrating Parallel Event Streams with Azure

**Project Link:** [View Project](http://nextwork.ai/projects/ai-azure-event-workflows)

**Author:** Khaoula Belhadj  
**Email:** belhadjkhaoula07@gmail.com

---

---

## Introducing Today's Project!

i'll stream alerts into an Azure Storage Queue and use Durable Function orchestration to update stats, send alerts, and log analytics, all at the same time.

### Key services and concepts

Set up a Storage Queue for alerts in your existing Storage Account

Verify your Function App can connect to the queue

### Challenges and wins

In this step, I'm creating a queue-triggered function to start orchestrations
Build the orchestrator function with fan-out pattern
Implement activity functions for stats, alerts, and logging

---

## Creating the Azure Storage Queue

by create an storage account

![Image](http://nextwork.ai/courageous_silver_serene_goblin/uploads/ai-azure-event-workflows_6t3k9m1v)

### Verifying the Function App connection

 the queue is in the same Storage Account

---

## Building the Durable Functions Orchestration

@app.queue_trigger: Tells Azure Functions to run this code whenever a message arrives in the specified queue

@app.durable_client_input: Injects a Durable Functions client that can start and manage orchestrations

@app.orchestration_trigger: Marks a function as an orchestrator that coordinates the workflow

@app.activity_trigger: Marks a function as an activity that does actual work

![Image](http://nextwork.ai/courageous_silver_serene_goblin/uploads/ai-azure-event-workflows_5w9t3h7j)

### Testing the fan-out orchestration

The statusQueryGetUri queries the orchestration status directly and always shows real-time results.

---

## Connecting Cosmos DB for Analytics

et up Cosmos DB for storing analytics data

Implement the stats aggregation endpoint

Test with simulated events

![Image](http://nextwork.ai/courageous_silver_serene_goblin/uploads/ai-azure-event-workflows_7m4k1p9s)

### Designing the container structure

Using separate variables makes it clear which database each part of my code connects

---

## Aggregating Real-Time Stream Statistics

I verified the stats aggregation by sending three simulated events to the deployed Azure Function: one follower, one subscription, and one donation of 10.00. I then queried /api/stats/stream123 to retrieve the aggregated statistics from Azure Cosmos DB.

![Image](http://nextwork.ai/courageous_silver_serene_goblin/uploads/ai-azure-event-workflows_6h3j9f1c)

---

## Implementing Production Error Handling

Add retry policies with exponential backoff to activity functions

Create a dead-letter queue for permanently failed events

Implement dead-letter handling in your orchestration

Test failure scenarios to verify your error handling works

### Dead-letter queue implementation

Event → Storage Queue → Function → Processing fails → Retry → Retry → Retry → Dead-letter queue

![Image](http://nextwork.ai/courageous_silver_serene_goblin/uploads/ai-azure-event-workflows_5c1g8e3w)

### Verifying retry and failure handling

The message contains everything you need to debug failures: the original_event that failed, the error message explaining why, and the orchestration_id to trace it in logs.

---

---
