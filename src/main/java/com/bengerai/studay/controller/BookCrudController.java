package com.bengerai.studay.controller;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.BooleanQuery;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 数据处理controller.
 */
@RestController
@RequestMapping("/bookcrud")
public class BookCrudController {

    /** logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BookCrudController.class);

    /**
     * es数据传输工具实例.
     */
    @Autowired
    private TransportClient client;

    @GetMapping("/get/book/novel")
    public ResponseEntity searchById(String id) {

        final GetResponse response = client.prepareGet("book", "novel", id).get();
        if (null == response || !response.isExists()) {
            return new ResponseEntity(HttpStatus.NOT_FOUND);

    }
        return new ResponseEntity(response.getSource(), HttpStatus.OK);
    }

    @PostMapping("/add/book/novel")
    public ResponseEntity add(@RequestParam("title") String title,
                               @RequestParam("author") String author,
                               @RequestParam("word_count") int wordCount,
                               @RequestParam("publish_date") @DateTimeFormat(pattern = "yyy-MM-dd HH:mm:ss") Date publishDate) {

        try {
            final XContentBuilder content = XContentFactory.jsonBuilder()
                    .field("title", title)
                    .field("author", author)
                    .field("word_count", wordCount)
                    .field("publish_date", publishDate)
                    .endObject();

            final IndexResponse response =
                    client.prepareIndex("book", "novel").setSource(content).get();
            return new ResponseEntity(response.getId(), HttpStatus.OK);
        } catch (IOException e) {
            LOGGER.error("add exception : ", e);
           return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }

    @PostMapping("/delete/book/novel")
    public ResponseEntity delete(String id) {

        final DeleteResponse response = client.prepareDelete("book", "novel", id).get();

        return new ResponseEntity(response.getResult(), HttpStatus.OK);
    }

    @PostMapping("/query/book/novel")
    public ResponseEntity query(@RequestParam(value = "title", required = false) String title,
                                @RequestParam(value = "author", required = false) String author,
                                @RequestParam(value = "word_count", required = false) Integer wordCount,
                                @RequestParam(value = "publish_date", required = false)
                                    @DateTimeFormat(pattern = "yyy-MM-dd HH:mm:ss") Date publishDate,
                                @RequestParam(value = "gt_word_count", defaultValue = "0") Integer gtWordCount,
                                @RequestParam(value = "lt_word_count", required = false) Integer ltWordCount) {

        //组装查询条件
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if (StringUtils.isNotBlank(title)) {
            boolQuery.must(QueryBuilders.matchQuery("title", title));
        }
        if (StringUtils.isNotBlank(author)) {
            boolQuery.must(QueryBuilders.matchQuery("author", author));
        }
        if (StringUtils.isNotBlank(author)) {
            boolQuery.must(QueryBuilders.matchQuery("word_count", wordCount));
        }
        if (StringUtils.isNotBlank(author)) {
            boolQuery.must(QueryBuilders.matchQuery("publish_date", publishDate));
        }

        // 以word_count作为条件范围
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("word_count").from(gtWordCount);
        if (ltWordCount != null && ltWordCount > 0) {
            rangeQuery.to(ltWordCount);
        }
        boolQuery.filter(rangeQuery);

        // 组装查询请求
        SearchRequestBuilder searchRequestBuilder =
                client.prepareSearch("book")
                .setTypes("novel")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQuery)
                .setFrom(0)
                .setSize(10);

        //发送查询请求
        final SearchResponse response = searchRequestBuilder.get();

        //组织查询到的数据
        final List<Map<String, Object>> result = new ArrayList<>();
        for (SearchHit searchHit : response.getHits()) {
            result.add(searchHit.getSource());
        }

        return new ResponseEntity(result, HttpStatus.OK);
    }
}
