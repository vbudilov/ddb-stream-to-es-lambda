package com.budilov.data.es.pojo

data class ElasticSearchResponse(

        val took: Int,
        val timed_out: Boolean,
        val hits: Hits
)
