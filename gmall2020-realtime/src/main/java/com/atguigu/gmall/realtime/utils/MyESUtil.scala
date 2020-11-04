package com.atguigu.gmall.realtime.utils

import java.util

import com.atguigu.gmall.realtime.bean.DauInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index, Search, SearchResult}

/**
 * @author Demigod_zhang
 * @create 2020-10-21 18:53
 */
object MyESUtil {
  private var factory:  JestClientFactory=null;

  def getJestClient():JestClient ={
    if(factory==null){
      build()
    };
    factory.getObject
  }

  def build(): Unit ={
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200")
      .multiThreaded(true)
    .maxTotalConnection(20)
    .connTimeout(10000)
    .readTimeout(1000)
    .build())
  }


  def putIndex(): Unit ={

    val jest: JestClient = getJestClient()

    val actorNameList = new util.ArrayList[String]()
    actorNameList.add("zhangsan")
    val index: Index = new Index.Builder(Movie("100","天龙八部",actorNameList))
      .index("movie_index")
      .`type`("movie")
      .id("1")
      .build()
    jest.execute(index)
    jest.close()
  }
  case class Movie(id:String ,movie_name:String, actorNameList: java.util.List[String] ){}


  def queryIndex(): Unit ={
    //获取操作对象
    val jest: JestClient = getJestClient()

    //查询常用有两个实现类 Get通过id获取单个Document，以及Search处理复杂查询
    val query =
      """
        |{
        |  "query": {
        |    "bool": {
        |       "must": [
        |        {"match": {
        |          "name": "red"
        |        }}
        |      ],
        |      "filter": [
        |        {"term": { "actorList.name.keyword": "zhang cuishan"}}
        |      ]
        |    }
        |  },
        |  "from": 0,
        |  "size": 20,
        |  "sort": [
        |    {
        |      "doubanScore": {
        |        "order": "desc"
        |      }
        |    }
        |  ],
        |  "highlight": {
        |    "fields": {
        |      "name": {}
        |    }
        |  }
        |}
    """.stripMargin
    val search: Search = new Search.Builder(query)
      .addIndex("movie_index")
      .build()

    val result: SearchResult = jest.execute(search)

    val rsList: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String,Any]])


    import scala.collection.JavaConverters._

    val list: List[util.Map[String, Any]] = rsList.asScala.map(_.source).toList
    println(list.mkString("\n"))

    jest.close()
  }


  def bulkInsert(infoList: List[(String,Any)], indexName: String): Unit = {

    if(infoList!=null && infoList.size!= 0){
      //获取客户端
      val jestClient = getJestClient()
      val bulkBuilder: Bulk.Builder = new Bulk.Builder()
      for ((id,dauInfo) <- infoList) {
        val index: Index = new Index.Builder(dauInfo)
          .index(indexName)
          .id(id)
          .`type`("_doc")
          .build()
        bulkBuilder.addAction(index)
      }
      //创建批量操作对象
      val bulk: Bulk = bulkBuilder.build()
      val bulkResult = jestClient.execute(bulk)
      println("向ES中插入"+bulkResult.getItems.size()+"条数据")
      jestClient.close()
    }
  }


  def main(args: Array[String]): Unit = {
    putIndex()
  }

}
