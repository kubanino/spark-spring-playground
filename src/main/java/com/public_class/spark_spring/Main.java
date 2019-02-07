package com.public_class.spark_spring;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class Main
{
    public static void main(String[] args)
    {
        final String logFile = "C:\\spark-2.2.1-bin-hadoop2.7\\README.md";
        final SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        final Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter(s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        stopMeUntilGodPressedEnter();

        spark.stop();
    }

    private static void stopMeUntilGodPressedEnter()
    {
        try
        {
            System.in.read();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}