package hahaha.lalala.UserProfile.nlp.hanlp

import com.hankcs.hanlp.corpus.io.{IIOAdapter, IOUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{FileInputStream, InputStream, OutputStream}
import java.net.URI

/**
 * @Descriptio 当用户自定义词典在HDFS上时，配置此IIOAdapter.
 *             当用户自定义词典在HDFS上找不到时，从HanLP jar包内读取默认的词典,该词典是精简模式的，也就是所谓的portable模式
 *             词条较少。一般生成环境需要自定义词典，可以添加自己业务相关的词条。HanLP也提供一些比较全的词典文件，这些文件不在JAR包中，
 *             我们将他们放到了HDFS上。
 *             操作步奏：
 *                     1. 在hdfs上创建一个词典目录  /common/nlp/data
 *                        2. 下载词典文件 http://doc.yihongyeyan.com/qf/project/soft/hanlp/hanlp.dictionary.tgz
 *                        3. 将下载后的文件解压后的文件夹整体上传到HDFS的 /common/nlp/data  目录下
 *                        4. 在hanlp.properties 配置文件中就可以按需配置了，默认如果能在HDFS上找到的字典会加载HDFS上字典，否则会加载JAR包内的
 *                        5. 要注意的是每次你添加新的词典数据，都要把以.bin结尾的文件删除，这样才会生效，hanlp源码的逻辑是，首先load .bin文件，如果
 *                        .bin 词典文件存在，直接加载。 不存在才会根据txt文件生成缓存。 因此你必须删除bin才能让你新添加的词生效
 * @Author niyaolanggeyo
 * @Date 2021/8/20 15:02
 * @Version 1.0
 */
class HadoopFileIOAdapter extends IIOAdapter {
  override def create(path: String): OutputStream = {
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(path), conf)
    fs.create(new Path(path))
  }

  override def open(path: String): InputStream = {
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(path), conf)
    if (fs.exists(new Path(path))) {
      fs.open(new Path(path))
    } else {
      if (IOUtil.isResource(path)) {
        IOUtil.getResourceAsStream("/" + path)
      } else {
        new FileInputStream(path)
      }
    }
  }
}


