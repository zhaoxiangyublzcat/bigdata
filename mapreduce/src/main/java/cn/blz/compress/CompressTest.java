package cn.blz.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

/**
 * @author Blzcat
 * @description 文件流压缩解压缩测试
 * @className CompressTest
 * @date 2019.12.30 15:58
 */
public class CompressTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        compress("", "org.apache.hadoop.io.compress.BZip2Codec");
        decompress("");
    }

    /**
     * @param fileName 文件名称
     * @description 数据流解压
     * @author Blzcat
     * @date 2019.12.30 16:38
     */
    private static void decompress(String fileName) throws IOException {
        CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = compressionCodecFactory.getCodec(new Path(fileName));
        if (codec == null) {
            return;
        }
        CompressionInputStream inputStream = codec.createInputStream(new FileInputStream(new File(fileName)));

        FileOutputStream outputStream = new FileOutputStream(fileName + ".decoded");

        IOUtils.copyBytes(inputStream, outputStream, 1024 * 5, false);

        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(outputStream);
    }

    /**
     * @param fileName 文件名称
     * @param method   压缩方法
     * @description 数据流压缩
     * @author Blzcat
     * @date 2019.12.30 16:33
     */
    private static void compress(String fileName, String method) throws IOException, ClassNotFoundException {
        FileInputStream fileInputStream = new FileInputStream(new File(fileName));

        Class<?> codeClass = Class.forName(method);
        CompressionCodec compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(codeClass, new Configuration());

        FileOutputStream fileOutputStream = new FileOutputStream(new File(fileName + compressionCodec.getDefaultExtension()));
        CompressionOutputStream compressionOutputStream = compressionCodec.createOutputStream(fileOutputStream);

        IOUtils.copyBytes(fileInputStream, compressionOutputStream, 1025 * 5, false);

        IOUtils.closeStream(fileInputStream);
        IOUtils.closeStream(fileOutputStream);
        IOUtils.closeStream(compressionOutputStream);
    }

}
