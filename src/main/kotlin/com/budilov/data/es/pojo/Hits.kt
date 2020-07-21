package com.budilov.data.es.pojo

data class ResponseMeta(
        val _index: String,
        val _type: String,
        val _id: String,
        val _source: Map<String, String>
)

data class Hits(

        val total: Total,
        val hits: List<ResponseMeta>
)
