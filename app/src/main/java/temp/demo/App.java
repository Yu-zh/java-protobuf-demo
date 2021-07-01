package temp.demo;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.google.protobuf.ByteString.copyFrom;

public class App {

  // serialize single message with the bytestring in the buffer
  static ByteString serializeWithBytes(ByteString bs) {
    withBytes.Builder builder = withBytes.newBuilder();
    withBytes msg = builder
        .setStr("foo")
        .setBts(bs)
        .setNum(42)
        .build();
    return msg.toByteString();
  }

  // serialize single message by appending the bytestring to the end
  static ByteString serializeWithoutBytes(ByteString bs) {
    withoutBytes.Builder builder = withoutBytes.newBuilder();
    withoutBytes msg = builder
        .setStr("foo")
        .setNum(42)
        .build();
    ByteString byteString = msg.toByteString();
    ByteString sizeInBytes = copyFrom(ByteBuffer.allocate(4).putInt(byteString.size()).array());
    return sizeInBytes.concat(byteString).concat(bs);
  }

  static void testSerializeWithBytes(List<ByteString> bss, FileOutputStream out) throws IOException {
    for (ByteString bs : bss) {
      serializeWithBytes(bs).writeTo(out);
      out.flush();
    }
  }

  static void testSerializeWithoutBytes(List<ByteString> bss, FileOutputStream out) throws IOException {
    for (ByteString bs : bss) {
      serializeWithoutBytes(bs).writeTo(out);
      out.flush();
    }
  }

  static void testDeSerializeWithBytes(List<String> fileNames, FileOutputStream out) throws IOException {
    for (String fileName: fileNames) {
      FileInputStream inputStream = new FileInputStream(fileName);
      withBytes msg = withBytes.parseFrom(inputStream);
      msg.writeTo(out);
      out.flush();
      inputStream.close();
    }
  }

  static void testDeserializeWithoutBytes(List<String> fileNames, FileOutputStream out) throws IOException {
    for (String fileName: fileNames) {
      FileInputStream inputStream = new FileInputStream(fileName);
      byte[] inputBA = new byte[inputStream.available()];
      inputStream.read(inputBA);
      ByteString bsMsg = copyFrom(inputBA);
      byte[] sizeInBA = bsMsg.substring(0,4).toByteArray();
      int size = ByteBuffer.wrap(sizeInBA).getInt();
      withoutBytes msg = withoutBytes.parseFrom(bsMsg.substring(4, 4 + size));
      msg.writeTo(out);
      bsMsg.substring(4 + size).writeTo(out);
      out.flush();
      inputStream.close();
    }
  }


  public static void testSerialize(int numBs, int sizeBs) throws IOException {
    ByteString[] bs = new ByteString[numBs];
    for (int i = 0; i < numBs; ++i) {
      byte[] b = new byte[sizeBs];
      new Random().nextBytes(b);
      bs[i] = copyFrom(b);
    }
    String fileName = "tmp";
    FileOutputStream outputStream = new FileOutputStream(fileName);
    List<ByteString> bss = new ArrayList<>(Arrays.asList(bs));

    System.out.println("build protocol buffer with bytes");
    long startTime = System.nanoTime();
    testSerializeWithBytes(bss, outputStream);
    long endTime = System.nanoTime();
    double duration = (endTime - startTime) / 1000000.0;
    System.out.println(duration / numBs);

    System.out.println("append bytes to the end");
    startTime = System.nanoTime();
    testSerializeWithoutBytes(bss, outputStream);
    endTime = System.nanoTime();
    duration = (endTime - startTime) / 1000000.0;
    System.out.println(duration / numBs);

    outputStream.close();
  }


  public static void testDeserialize(int numBs, int lenBs) throws IOException {
    String fileName = "tmp";
    String[] filesWithBytes = new String[numBs];
    String[] filesWithoutBytes = new String[numBs];
    FileOutputStream outputStream;
    for (int i = 0; i < numBs; ++i) {
      byte[] b = new byte[lenBs];
      new Random().nextBytes(b);
      ByteString bs = copyFrom(b);
      filesWithBytes[i] = fileName.concat(Integer.toString(i)).concat(".with");
      filesWithoutBytes[i] = fileName.concat(Integer.toString(i)).concat(".without");

      outputStream = new FileOutputStream(filesWithBytes[i]);
      serializeWithBytes(bs).writeTo(outputStream);
      outputStream.flush();
      outputStream.close();

      outputStream = new FileOutputStream(filesWithoutBytes[i]);
      serializeWithoutBytes(bs).writeTo(outputStream);
      outputStream.flush();
      outputStream.close();
    }

    List<String> fileList;
    FileOutputStream out = new FileOutputStream(fileName);
    System.out.println("build protocol buffer with bytes");
    fileList = new ArrayList<>(Arrays.asList(filesWithBytes));
    long startTime = System.nanoTime();
    testDeSerializeWithBytes(fileList, out);
    long endTime = System.nanoTime();
    double duration = (endTime - startTime) / 1000000.0;
    System.out.println(duration/numBs);

    System.out.println("append bytes to the end");
    fileList = new ArrayList<>(Arrays.asList(filesWithoutBytes));
    startTime = System.nanoTime();
    testDeserializeWithoutBytes(fileList, out);
    endTime = System.nanoTime();
    duration = (endTime - startTime) / 1000000.0;
    System.out.println(duration/numBs);
    out.close();
  }

  public static void main(String args[]) throws IOException {

    // serialize
    int numBs = 1000;
    int[] bsLens = new int[] {1, 10, 100, 1000};
//    for (int len : bsLens) {
//      System.out.print("test on size (kb): ");
//      System.out.println(len);
//      testSerialize(numBs, len * 1000);
//    }
//    numBs = 100;
//    bsLens = new int[] {10};
//    for (int len : bsLens) {
//      System.out.print("test on size (mb): ");
//      System.out.println(len);
//      testSerialize(numBs, len * 1000000);
//    }
//    numBs = 10;
//    bsLens = new int[] {100};
//    for (int len : bsLens) {
//      System.out.print("test on size (mb): ");
//      System.out.println(len);
//      testSerialize(numBs, len * 1000000);
//    }

    // deserialize
    numBs = 1000;
    bsLens = new int[] {1, 10, 100, 1000};
    for (int len : bsLens) {
      System.out.print("test on size (kb): ");
      System.out.println(len);
      testDeserialize(numBs, len * 1000);
    }
    numBs = 100;
    bsLens = new int[] {10};
    for (int len : bsLens) {
      System.out.print("test on size (mb): ");
      System.out.println(len);
      testDeserialize(numBs, len * 1000000);
    }
    numBs = 10;
    bsLens = new int[] {100};
    for (int len : bsLens) {
      System.out.print("test on size (mb): ");
      System.out.println(len);
      testDeserialize(numBs, len * 1000000);
    }
  }
}
