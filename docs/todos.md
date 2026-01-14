## Quality

### UnifiedExceptionHandling

Reporting and logging exceptions should be a uniform process I think the original intent was to use the system topic for this. This seems like a good plan, but hasn't really been properly implemented yet.

Need a specification for this, need to re-examine existing code and test it out.

### Dead Letter Queue

Need a pattern for handling messages that fail ingest or transformation

### Clean Shutdown of Kafka

When a Genegraph app is stopped, we need to make sure that the transactional ids for producers and the group ids for consumers are releaed, so that the next instance starting can use them.

### Kafka robustness

Should report errors and automatically attempt to restart consumers and producers that have hit an error state. Optionally consider flagging the instance for deletion/reinitialization by kubernetes as a last resort.

Related are kafka architecture improvements. Whether topics should own Kafka Consumers is up for debate (am leaning towards yes), but having Kafka Producer code in the Processor is not great. I also want to be able to batch transactional commits; the need for a transaction commit per message is a major drag on processing speed.
