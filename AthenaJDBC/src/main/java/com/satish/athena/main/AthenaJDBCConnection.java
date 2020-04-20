package com.satish.athena.main;

import java.sql.*;
import java.util.Properties;

public class AthenaJDBCConnection {

    private static final String CONNECTION_URL = "jdbc:awsathena://AwsRegion=us-east-1";
    private static final String DRIVER_CLASS = "com.simba.athena.jdbc.Driver";
    private static final String S3_OUTPUT_LOCTAION = "s3://satish-athena/athena-results/";

    private String accessKeyId;
    private String secretKey;
    private String databaseName;

    public AthenaJDBCConnection(String accessKeyId, String secretKey, String databaseName) {
        this.accessKeyId = accessKeyId;
        this.secretKey = secretKey;
        this.databaseName =databaseName;
    }

    public void execute() {
        Properties properties = new Properties();
        // Set System Properties
        System.setProperty("aws.accessKeyId", accessKeyId);
        System.setProperty("aws.secretKey", secretKey);

        // Set Athena Properties
        properties.setProperty("S3OutputLocation", S3_OUTPUT_LOCTAION);

        // Using DefaultAWSCredentialsProviderChain
        properties.setProperty("AwsCredentialsProviderClass", "com.simba.athena.amazonaws.auth.SystemPropertiesCredentialsProvider");

        Connection conn = null;
        Statement statement = null;

        try {
            Class.forName(DRIVER_CLASS);
            System.out.println("Connecting to Athena...");
            conn = DriverManager.getConnection(CONNECTION_URL, properties);
            statement = conn.createStatement();

            System.out.println("Execute Query ");
            String query = "show tables in "+ databaseName;
            ResultSet rs = statement.executeQuery(query);
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            System.out.println("Print Query REsults");
            while(rs.next()) {
                System.out.println(rs.getString(1));
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if(conn != null) {
                try {
                    conn.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }

                if(statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            }
        }

    }

    public static void main( String[] args ) {

        if(args.length == 0 || args.length != 3) {
            System.out.println("Invalid Arguments passed.");
        } else {
            String accessKeyId = args[0];
            String secretKey = args[1];
            String databaseName = args[2];

            AthenaJDBCConnection jdbcConnection = new AthenaJDBCConnection(accessKeyId, secretKey, databaseName);
            jdbcConnection.execute();
        }
    }
}
