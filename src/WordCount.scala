import java.io.File

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer


/**
 * Created by zhuang on 15-7-8.
 * 提交Application到spark集群
 * cd ${SPARK_HOME}/bin
 * ./spark-submit [--master MASTER URI
 * --class Main Class
 * ... Other Agrs]
 * Application.jar
 * [... Cmd Agrs]
 *
 */

object WordCount {
  /**
   * agrs[0]：需统计单词的路径 可以是文件路径也可以是目录路径 默认"/home/zhuang/spark/bin"
   */
  def main(args: Array[String]) {
    val filePath = if(args.length > 0) args(0) else "/home/zhuang/spark/bin"
    val files = FileUtils.listAllFiles(new File(filePath))

    //firstSubmit(filePath)
    //secondSubmit(files)
    thirdSubmit(filePath)
  }

  /**
   * 使用textFile提交
   * textFile的参数是一个path,这个path可以是：
   * 1)一个文件路径，这时候只装载指定的文件
   * 2)一个目录路径，但是该目录下不能有子目录，而且有多少个输入源就有多少个输出文件，但却是对所有文件统计
   * 3)也可以接受commaSeparatedPaths，即逗号分割的路径字符串，
   * 如："/home/zhuang/spark/README.md,/home/zhuang/spark/NOTICE"
   * 也是对所有文件统计，输出文件数量跟输入数量相同
   * @param file
   */
  def firstSubmit(file : String) = {
    val conf = new SparkConf().setAppName("Scala_WordCount")
                              .setMaster("spark://localhost:7077")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(file)
    textFile.flatMap(_ .split(" ")).map((_ , 1)).reduceByKey(_ + _).saveAsTextFile("/home/zhuang/result")
  }

  /**
   * 使用textFile提交
   * 每个文件都提交一次，需要解决目录问题，否则会报目录已存在错误
   * 当然这种方法很不合理
   * @param files
   */
  def secondSubmit(files : List[File]) = {
    val conf = new SparkConf().setAppName("Scala_WordCount")
                              .setMaster("spark://localhost:7077")
    val sc = new SparkContext(conf)

    for(file <- files){
      val textFile = sc.textFile(file.getPath)
      textFile.flatMap(_ .split(" ")).map((_ , 1)).reduceByKey(_ + _).saveAsTextFile("/home/zhuang/result/" + file.getName)
    }
  }

  /**
   * 使用wholeTextFiles提交，一次读取多个数据文件
   * 返回Key/Value,即元组，Key：文件路径 Value：文件内容
   * 但不会像textFile那样输出多个数据文件
   * 并且会自动忽略目录下的子目录，只会读取该目录下的所有文件
   * @param file
   *
   * mapPartitions是map的一个变种。map的输入函数是应用于RDD中每个元素，
   * 而mapPartitions的输入函数是应用于每个分区，也就是把每个分区中的内容作为整体来处理的。
   *
   * 函数定义：
   * def mapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = false): RDD[U]
   *
   * f即为输入函数，它处理每个分区里面的内容。
   * 每个分区中的内容将以Iterator[T]传递给输入函数f，f的输出结果是Iterator[U]。
   * 最终的RDD由所有分区经过输入函数处理后的结果合并起来的。
   *
   * e.g.
   * scala> val a = sc.parallelize(1 to 9, 3)
   * scala> def myfunc[T](iter: Iterator[T]) : Iterator[(T, T)] = {
   * var res = List[(T, T)]()
   * var pre = iter.next while (iter.hasNext) {
   *     val cur = iter.next;
   *     res .::= (pre, cur) pre = cur;
   * }
   * res.iterator
   * }
   * scala> a.mapPartitions(myfunc).collect
   * res0: Array[(Int, Int)] = Array((2,3), (1,2), (5,6), (4,5), (8,9), (7,8))
   *
   */
  def thirdSubmit(file : String) = {
    def myFunc[T](iterator : Iterator[(T, T)]) : Iterator[T] = {
      val res = ListBuffer[T]()
      while (iterator.hasNext){
        res.append(iterator.next()._2)
      }
      res.toList.iterator
    }
    val conf = new SparkConf().setAppName("Scala_WordCount")
                              .setMaster("spark://localhost:7077")
    val sc = new SparkContext(conf)

    val wholeTextFiles = sc.wholeTextFiles(file)
    // 第一种方式使用wholeTextFiles
    //wholeTextFiles.mapPartitions(myFunc).flatMap(_ .split(" ")).map((_ , 1))
    //                                    .reduceByKey(_ + _).saveAsTextFile("/home/zhuang/result")

    // 第二种方式使用wholeTextFiles
    wholeTextFiles.flatMap(_ ._2.split(" ")).map((_ , 1)).reduceByKey(_ + _).saveAsTextFile("/home/zhuang/result")
  }
}

object FileUtils {
  /**
   * 遍历目录下所有文件
   * @param file file可能是文件也可能是文件夹
   * @return
   */
  def listAllFiles(file: File): List[File] = {
    val lists = ListBuffer[File]()
    if (file.isFile) lists.append(file)
    else listAllFiles(file, lists)
    lists.toList
  }

  /**
   * 递归遍历
   * @param file 只能是文件夹
   * @param lists
   * @return
   */
  private def listAllFiles(file: File, lists: ListBuffer[File]): List[File] = {
    for (child <- file.listFiles()) {
      if (child.isFile && !child.getName.endsWith(".zip")) lists.append(child)
      else if (child.isDirectory) listAllFiles(child, lists)
    }
    lists.toList
  }
}