package com.budilov.data

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.budilov.data.es.ElasticSearchDAO
import org.slf4j.LoggerFactory

class DynamoDBStreamLambda : RequestHandler<DynamodbEvent, String> {
    private val logger = LoggerFactory.getLogger("DynamoDBStreamLambda")
    private val esService = ElasticSearchDAO()

    override
    fun handleRequest(ddbEvent: DynamodbEvent, context: Context): String {

        ddbEvent.records.forEach { record ->
            logger.debug("Record $record")

            val map = mutableMapOf<String, String>()
            val id = record.dynamodb.keys["id"]?.s
            var sortKey = record.dynamodb.keys["sortKey"]?.s
            if (sortKey == null)
                sortKey = "default"

            logger.info("${record.eventName}: $id in $sortKey")

            record.dynamodb?.newImage?.entries?.forEach {
                val key = it.key
                val value = it.value.s

                logger.debug("${record.eventName} - $key -> $value")

                map[key] = value
            }

            when (record.eventName?.toLowerCase()) {
                "insert" -> {
                    if (id != null) {
                        val success = esService.add(sortKey, id, map)
                        logger.info("inserted: $success")
                    }
                }
                "modify" -> {
                    if (id != null) {
                        val success = esService.update(sortKey, id, map)
                        logger.info("modified: $success")

                    }
                }

                "remove" -> {
                    if (id != null) {
                        val success = esService.delete(sortKey, id)
                        logger.info("removed: $success")
                    }
                }
            }
        }

        return "Successfully processed " + ddbEvent.records.size + " records."
    }
}
