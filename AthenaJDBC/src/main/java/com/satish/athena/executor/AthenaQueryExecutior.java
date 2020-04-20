package com.satish.athena.executor;

import java.sql.*;
import java.util.Properties;

import static com.satish.athena.executor.AthenaUtils.*;

public class AthenaQueryExecutior {

    enum  CREDENTIALS_PROVIDER_TYPES  { DEFAULTAWS, PROPERTIESFILE , INSTANCEPROFILE, CUSTOMSESSION, EXTERNALACCOUNT}

    private String connectionURL = null;
    private String driverClass = null;
    private String athenaQueryResultBucket = null;
    private String authType= null;
    private String authParams= null;
    private String athenaQuery = null;

    public AthenaQueryExecutior (String connectionURL, String driverClass, String athenaQueryResultBucket, String authType, String authParams, String athenaQuery) {
        this.connectionURL = connectionURL;
        this.driverClass = driverClass;
        this.athenaQueryResultBucket = athenaQueryResultBucket;
        this.authType= authType;
        this.authParams= authParams;
        this.athenaQuery = athenaQuery;
    }

    public void execute() {

        Properties properties = getAuthenticationProperties(authType,authParams);
        properties.setProperty("S3OutputLocation", athenaQueryResultBucket);
        System.out.println("AwsCredentialsProviderClass = "+properties.getProperty("AwsCredentialsProviderClass"));

        Connection conn = null;
        Statement statement = null;

        try {
            Class.forName(driverClass);
            System.out.println("Connecting to Athena...");
            conn = DriverManager.getConnection(connectionURL, properties);
            statement = conn.createStatement();

            System.out.println("Execute Query ");
            ResultSet rs = statement.executeQuery(athenaQuery);
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            System.out.println("Print Query REsults");
            while(rs.next()) {
                System.out.println(rs.getString(1)+"\t" + rs.getBigDecimal(2));
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
    public void execute1() {
        //Properties properties = new Properties();
        Connection conn = null;
        Statement statement = null;

        Properties properties = null;
        try {
            properties = getAuthenticationProperties(authType,authParams);
            properties.setProperty("S3OutputLocation", athenaQueryResultBucket);
        } catch (Exception e) {
            System.out.println("Invalid authType");
            e.printStackTrace();
        }

        try {
            Class.forName(driverClass);
            System.out.println("Get Connection");
            conn = DriverManager.getConnection(connectionURL, properties);
            statement = conn.createStatement();

            System.out.println("Execute Query ");
            ResultSet rs = statement.executeQuery(athenaQuery);
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            System.out.println("Print Query REsults");
            while(rs.next()) {
                System.out.println(rs.getString(1)+"\t" + rs.getBigDecimal(2));
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


    public static Properties getAuthenticationProperties(String authType,String authParams)  {
        System.out.println(">>>>> getAuthenticationProperties <<<<");
        CREDENTIALS_PROVIDER_TYPES credentialProviderType = CREDENTIALS_PROVIDER_TYPES.valueOf(authType);

        System.out.println("authType = "+authType);
        System.out.println("authParam = "+authParams);
        System.out.println("credentialProviderType = "+credentialProviderType);

        switch (credentialProviderType) {
            case DEFAULTAWS :
                System.out.println("DEFAULTAWS");
                return getPropertiesWithDefaultAWSCredentialsProviderChain();
            case PROPERTIESFILE:
                System.out.println("PROPERTIESFILE");
                return getPropertiesWithPropertiesFileCredentialsProvider(authParams);
            case INSTANCEPROFILE:
                System.out.println("INSTANCEPROFILE");
                return getPropertiesWithInstanceProfileCredentialsProvider();
            case CUSTOMSESSION:
                System.out.println("CUSTOMSESSION");
                return getPropertiesWithCustomSessionCredentialsProvider(authParams);
            case EXTERNALACCOUNT:
                System.out.println("EXTERNALACCOUNT");
                return getPropertiesWithExternalAccountCredentialsProvider(authParams);
        }
        return null;
    }



}
