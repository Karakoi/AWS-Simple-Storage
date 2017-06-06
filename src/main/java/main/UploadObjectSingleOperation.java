package main;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class UploadObjectSingleOperation {
    private static String bucketName = "karakoibucket";
    private static String keyName = "myItem1";
    private static String uploadFileName = "E:\\Документи\\Git - repositories\\AWS_S3\\src\\main\\resources\\item1.txt";

    public static void main(String[] args) throws IOException {
        AWSCredentials credentials = new BasicAWSCredentials("***", "***");
//        uploadFile(credentials);
//        readFile(credentials);
        deleteFile(credentials);
    }

    public static void uploadFile(AWSCredentials credentials) {
//        AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());
        AmazonS3 s3client = new AmazonS3Client(credentials);
        try {
            System.out.println("Uploading a new object to S3 from a file...\n");
            File file = new File(uploadFileName);
            s3client.putObject(new PutObjectRequest(
                    bucketName, keyName, file));
            System.out.println("Done)\n");

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
                    "means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
                    "means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void deleteFile(AWSCredentials credentials) {
        AmazonS3 s3client = new AmazonS3Client(credentials);
        try {
            System.out.println("Deleting object from S3...\n");
            s3client.deleteObject(new DeleteObjectRequest(bucketName, "i.jpg"));
            System.out.println("Done)\n");

        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
                    "means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    public static void readFile(AWSCredentials credentials) {
        AmazonS3 s3client = new AmazonS3Client(credentials);
        try {
            System.out.println("Downloading object from S3...\n");
            S3Object s3Object = s3client.getObject(new GetObjectRequest(bucketName, keyName));
            System.out.println("Done)\n");
            System.out.println("Content-Type: " + s3Object.getObjectMetadata().getContentType());

            S3ObjectInputStream is = s3Object.getObjectContent();
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader reader = new BufferedReader(isr);
            String currentLine;
            try {
                while ((currentLine = reader.readLine()) != null) {
                    System.out.println(currentLine);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (AmazonClientException ace) {
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

}
