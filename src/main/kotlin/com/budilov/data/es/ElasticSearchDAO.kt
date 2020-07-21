package com.budilov.data.es

import com.amazon.sig4.AWSRequestSigningApacheInterceptor
import com.amazonaws.auth.AWS4Signer
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import org.apache.http.HttpHost
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.Operator
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.FieldSortBuilder
import org.elasticsearch.search.sort.SortOrder
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.TimeUnit

enum class ResultSortOrder {
    /**
     * Ascending order.
     */
    ASC {
        override fun toString(): String {
            return "asc"
        }
    },

    /**
     * Descending order.
     */
    DESC {
        override fun toString(): String {
            return "desc"
        }
    };

}

class ElasticSearchDAO {
    private val log = LoggerFactory.getLogger("ElasticSearchDAO")

    private val esEndpoint = System.getenv("esUrl")

    private val esClient: RestHighLevelClient


    init {
        esClient = esClient("es", "us-east-1")
    }

    fun add(indexName: String, docId: String? = UUID.randomUUID().toString(), map: Map<String, Any>): Boolean {
        log.info("Adding $docId in $indexName")

        val request = IndexRequest(indexName)
                .id(docId)
                .source(map)

        val indexResponse = esClient.index(request, RequestOptions.DEFAULT)
        log.info("added code: $indexResponse")

        return indexResponse.result == DocWriteResponse.Result.CREATED
    }

    fun delete(indexName: String, docId: String): Boolean {
        log.info("Deleting $docId in $indexName")
        val request = DeleteRequest(
                indexName,
                docId)

        val deleteResponse = esClient.delete(
                request, RequestOptions.DEFAULT)

        log.info("deleted code: $deleteResponse")

        return deleteResponse.result == DocWriteResponse.Result.DELETED
    }

    fun update(indexName: String, docId: String, document: Map<String, Any>): Boolean {
        log.info("Updating $docId in $indexName")

        val indexRequest: IndexRequest = IndexRequest(indexName)
                .id(docId)
                .source(document)
        val updateRequest: UpdateRequest = UpdateRequest(indexName, docId)
                .doc(document)
                .upsert(indexRequest)

        val updateResponse = esClient.update(updateRequest, RequestOptions.DEFAULT)

        log.info("Updated code: $updateResponse")

        return updateResponse.result == DocWriteResponse.Result.UPDATED
    }

    /**
     * Just returns one item since we're searching by docId
     *
     */
    fun getOne(indexName: String, docId: String): Any? {
        val getRequest = GetRequest(
                indexName,
                docId)
        val getResponse = esClient.get(getRequest, RequestOptions.DEFAULT)
        log.debug(getResponse.sourceAsString)
        return getResponse.sourceAsString
    }

    /**
     * A very complicated function that allows for complicated searches
     *
     */
    fun search(indexName: String, query: String, searchFields: Array<String?>,
               filterFields: Array<Map<String, String>>? = null,
               limitRecords: Int? = 30,
               offset: Int? = null,
               sortBy: String? = null,
               sortDirection: ResultSortOrder? = null): Pair<Long, Array<SearchHit>> {

        if (query.isNullOrEmpty()) {
            return Pair(0, emptyArray())
        }

        val sourceBuilder = SearchSourceBuilder()
        sourceBuilder.query(BoolQueryBuilder()
                .must(QueryBuilders
                        .multiMatchQuery(query, *searchFields)
                        .operator(Operator.AND)))

        // Add the filters
        filterFields?.forEach { map ->
            map.forEach { (t, u) ->
                sourceBuilder.postFilter(QueryBuilders.termQuery(t, u))
            }
        }
        sourceBuilder.timeout(TimeValue(15, TimeUnit.SECONDS))

        // Sort By
        if (!sortBy.isNullOrBlank() && sortDirection != null)
            sourceBuilder.sort(FieldSortBuilder(sortBy).order(SortOrder.valueOf(sortDirection.name)))

        // Offset
        if (offset != null)
            sourceBuilder.from(offset)

        sourceBuilder.size((limitRecords) ?: 30)

        val searchRequest = SearchRequest()
        searchRequest.indices(indexName)
        searchRequest.source(sourceBuilder)


        val response = esClient.search(searchRequest, RequestOptions.DEFAULT)
        log.debug("response: $response")
        val hits = response.hits.hits ?: emptyArray()
        val hitMap = mutableListOf<Map<String, Any?>>()
        response.hits.hits.forEach {
            hitMap.add(it.sourceAsMap)
        }

        return Pair(response.hits.totalHits.value, hits)
    }

    fun wildcard(indexName: String, query: String, field: String): Pair<Long, Any> {
        val sourceBuilder = SearchSourceBuilder()
        sourceBuilder.query(QueryBuilders.wildcardQuery(field, query))
        sourceBuilder.timeout(TimeValue(15, TimeUnit.SECONDS))

        val searchRequest = SearchRequest()
        searchRequest.indices(indexName)
        searchRequest.source(sourceBuilder)
        val response = esClient.search(searchRequest, RequestOptions.DEFAULT)
        return Pair(response.hits.totalHits.value, response)
    }

    private fun dropIndex(indexName: String) {
        val request = DeleteIndexRequest(indexName)
        val deleteIndexResponse = esClient.indices().delete(request, RequestOptions.DEFAULT)
    }

    /**
     * Get the http client and add a Sig4 interceptor to it
     */
    private fun esClient(serviceName: String, region: String): RestHighLevelClient {
        val signer = AWS4Signer()
        signer.serviceName = serviceName
        signer.regionName = region
        val interceptor = AWSRequestSigningApacheInterceptor(serviceName, signer, DefaultAWSCredentialsProviderChain())
        return RestHighLevelClient(RestClient.builder(HttpHost.create(esEndpoint)).setHttpClientConfigCallback { hacb -> hacb.addInterceptorLast(interceptor) })
    }

    fun destroyClient() {
        esClient.close()
    }
}

fun main(args: Array<String>) {
    val log = LoggerFactory.getLogger("ElasticSearchDAO-main")


    val uuid = UUID.randomUUID().toString()
    val esDao = ElasticSearchDAO()
//    val podcastDAO = PodcastDAO()

//    val map = mutableMapOf<String, String>()
//    map["type"] = "episode"
//    map["id"] = uuid
//    map["sortKey"] = "podcast_${uuid}"
//    println(esDao.add(indexName = AppProperties.ES_INDEX, document = map))
//    val (_, result) = esDao.search(indexName = AppProperties.ES_INDEX, query = "podcast", fields = emptyArray())
//
//    result.forEach { item ->
//        log.info("$item")
//
//        esDao.delete(indexName = AppProperties.ES_INDEX, docId = item._id)
//    }
//    esDao.search(indexName = AppProperties.ES_PODCAST_INDEX, query = "podcast",
//            searchFields = emptyArray(), filterFields = emptyArray()).second.forEach {
//        log.info("${it.id} - ${it.sourceAsMap.get("rssFeedLink")}")
//    }
//    log.info(esDao.search(indexName = AppProperties.ES_INDEX, query = "podcast", fields = emptyArray(), filterFields = arrayOf(mapOf("type" to "episode"))).toString())
//
//    log.info(esDao.search(AppProperties.ES_PODCAST_INDEX, "", emptyArray()).toString())
//    log.info(esDao.search(AppProperties.ES_INDEX, "27fed04f-9b6b-4b65-bd5f-6d1b2c824b52", emptyArray()).toString())
//    log.info(esDao.search(AppProperties.ES_INDEX, "Jordan", emptyArray()).toString())
//    log.info(esDao.getOne(AppProperties.ES_INDEX, "27fed04f-9b6b-4b65-bd5f-6d1b2c824b52").toString())
//    log.info(esDao.getOne(AppProperties.ES_PODCAST_INDEX, "e85d33dcf713525c2bb3236533fd6b35").toString())
//    esDao.delete(AppProperties.ES_INDEX, "podcastf6490ff7c7be99d794ae156634594a7f")

//    val result = esDao.search(indexName = AppProperties.ES_EPISODES_INDEX,
//            query = "e85d33dcf713525c2bb3236533fd6b35",
//            searchFields = arrayOf("podcastId"))
//
//    log.info(result.first.toString())
//    val result = esDao.getOne(AppProperties.ES_EPISODES_INDEX, "18f30379a9f41a700f59acdf1a3f35d0")
//    log.info(result.toString())
//    esDao.dropIndex(AppProperties.ES_PODCAST_INDEX)
//    esDao.dropIndex(AppProperties.ES_EPISODES_INDEX)

    esDao.destroyClient()

}
