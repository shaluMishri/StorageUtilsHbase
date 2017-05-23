package com.storage.opr;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

public class StorageUtilsHbase {

	public static void main(String[] args) throws IOException {
		
		long startTime = System.currentTimeMillis();
		String actionToPrint = null;
			ArgumentParser parser = ArgumentParsers.newArgumentParser("StorageUtils").description("Storage Utility");
			String cloudStorage=null;
			String AccountName = null;
			String objectInBucket = null;
			String fileName=null;
			String action = null;
			String accessKey = null;
			String secretKey = null;
			String proxy = null;
			Integer port = 0;
			try {
				parser.addArgument("-cloudStorage","--cloudStorage").required(true);
				parser.addArgument("-AccountName", "--AccountName").required(true);
				parser.addArgument("-objectInBucket", "--objectInBucket").required(true);
				parser.addArgument("-fileName", "--fileName").required(true);
				parser.addArgument("-action", "--action").required(true);
				parser.addArgument("-accessKey", "--accessKey").required(false);
				parser.addArgument("-secretKey", "--secretKey").required(false);
				parser.addArgument("-proxy", "--proxy").required(false);
				parser.addArgument("-port", "--port").setDefault(0).type(Integer.class).required(false);

				cloudStorage=parser.parseArgs(args).getString("cloudStorage");
				AccountName = parser.parseArgs(args).getString("AccountName");
				objectInBucket = parser.parseArgs(args).getString("objectInBucket");
				fileName = parser.parseArgs(args).getString("fileName");
				action = parser.parseArgs(args).getString("action");
				accessKey = parser.parseArgs(args).getString("accessKey");
				secretKey = parser.parseArgs(args).getString("secretKey");
				action = parser.parseArgs(args).getString("action");
				proxy = parser.parseArgs(args).getString("proxy");
				port = Integer.parseInt(parser.parseArgs(args).getString("port"));		
			} catch (ArgumentParserException e) {
				e.printStackTrace();
				System.out.println("Exception: " + e);
				System.out.println("Error Message: " + e.getMessage());
				System.exit(-1);
			}
			 String storageConnectionString = "DefaultEndpointsProtocol=https;"+
						"AccountName="+AccountName+";"+
						"AccountKey="+accessKey;
		
				if(cloudStorage.equalsIgnoreCase("AZURE-STORAGE")){
					try{
					 CloudStorageAccount account = CloudStorageAccount.parse(storageConnectionString);
						CloudBlobClient serviceClient = account.createCloudBlobClient();
						// Container name must be lower case.Create if not exist
						CloudBlobContainer container = serviceClient.getContainerReference(objectInBucket);
						// container.createIfNotExists();
						CloudBlockBlob blob = container.getBlockBlobReference(fileName);
						File sourceFile = new File(fileName);
					if(action.equalsIgnoreCase("upload"))
					{
						System.out.println("Action is UPLOAD: " + action.equalsIgnoreCase("UPLOAD"));
						// Upload an file.
			            blob.upload(new FileInputStream(sourceFile), sourceFile.length());
			        
					}else if(action.equalsIgnoreCase("download"))
					{
						 // Download the file.
						System.out.println("Action is DOWNLOAD:  " + action.equalsIgnoreCase("DOWNLOAD"));
			            File destinationFile = new File(sourceFile.getParentFile(), fileName);
			            blob.downloadToFile(destinationFile.getAbsolutePath());
			        }
				}
				catch (FileNotFoundException fileNotFoundException) {
						fileNotFoundException.printStackTrace();
						System.out.print("FileNotFoundException encountered: ");
						System.out.println(fileNotFoundException.getMessage());
						System.exit(-1);
					} catch (StorageException storageException) {
						storageException.printStackTrace();
						System.out.print("StorageException encountered: ");
						System.out.println(storageException.getMessage());
						System.exit(-1);
					} catch (Exception e) {
						e.printStackTrace();
						System.out.print("Exception encountered: ");
						System.out.println(e.getMessage());
						System.exit(-1);
				}  	}
				else if(cloudStorage.equalsIgnoreCase("AWS-S3")){
					try{
					StorageUtilsHbase s3UtilObject = new StorageUtilsHbase();
					if (action.equals("UPLOAD")) {
						System.out.println("Action is UPLOAD: " + action.equals("UPLOAD"));
						String objectKey = AccountName + "/";
						File compressedfile = new File(fileName);
						s3UtilObject.uploadSSEFileToS3(accessKey, secretKey, AccountName, objectInBucket, objectKey,
								compressedfile, proxy, port);
						actionToPrint = "UPLOAD";

					} else if (action.equals("DOWNLOAD")) {
						System.out.println("Action is DOWNLOAD:  " + action.equals("DOWNLOAD"));
						String objectKeyForDownload = AccountName + "/" + fileName;
						s3UtilObject.downloadSSEFileFromS3(accessKey, secretKey,  AccountName,objectInBucket, objectKeyForDownload,
								fileName, proxy, port);
						actionToPrint = "DOWNLOAD";
					} else if (action.equals("DELETE")) {
						System.out.println("Action is DELETE: " + action.equals("DELETE"));
						s3UtilObject.deleteFileFromS3( AccountName,objectInBucket, fileName, accessKey, secretKey, proxy, port);
						actionToPrint = "DELETE";
					}
					}
					 catch (Exception e) {
						 e.printStackTrace();
						 System.out.println(e.getMessage());
						 System.exit(-1);
				
					 } 
					
	}
				System.out.println("Total Run time for Action:" + actionToPrint + " "+ ((System.currentTimeMillis() - startTime) / 1000) + " Sec");
				System.exit(0);
	}
	/**
	 * method to upload File to S3 in AWS
	 * 
	 * @param bucketName
	 * @param objectKey
	 * @param accessKey
	 * @param secretKey
	 * @param compressedfile
	 * @param retentionDays
	 * @param port
	 * @param proxy
	 * @param objectKeyName
	 * @throws IOException
	 */

	public void uploadSSEFileToS3(String accessKey, String secretKey, String bucketName, String objectKeyName,
			String objectKey,File compressedfile, String proxy, Integer port)
			throws IOException {
		try {
		AmazonS3 s3Obj;
		BasicAWSCredentials awsCreds;
		if (accessKey.equalsIgnoreCase("NA") && secretKey.equalsIgnoreCase("NA")) {
			s3Obj = AmazonS3ClientBuilder.standard().withCredentials(InstanceProfileCredentialsProvider.getInstance())
					.build();
		} else {
			awsCreds = new BasicAWSCredentials(accessKey, secretKey);
			ClientConfiguration cc = new ClientConfiguration();
			if ((proxy != null) && (port != null) || ((proxy != null) && (port != 0))) {
				cc.setProxyHost(proxy);
				cc.setProxyPort(port);
				s3Obj = new AmazonS3Client(awsCreds, cc);
			} else {
				s3Obj = new AmazonS3Client(awsCreds);
			}
		}
		PutObjectRequest putRequest1 = new PutObjectRequest(bucketName, objectKey + compressedfile, compressedfile);

		// Request server-side encryption.
		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		putRequest1.setMetadata(objectMetadata);

		System.out.println(
					"Uploading file: " + compressedfile + " in key " + objectKey + " to S3 bucket " + bucketName);
			PutObjectResult response1 = s3Obj.putObject(putRequest1);
			System.out.println("Uploaded object encryption status is " + response1.getSSEAlgorithm());

		} catch (AmazonClientException ace) {
			ace.printStackTrace();
			System.out.println("Exception: " + ace);
			System.out.println(
					"Caught an AmazonClientException in Uploading File, which means the client encountered an internal error while trying to communicate with S3");
			System.out.println("Error Message: " + ace.getMessage());
		}

	}

	/**
	 * Method to download File from S3
	 * 
	 * @param accessKey
	 * @param mySecretKey
	 * @param bucketName
	 * @param objectKey
	 * @param proxy
	 * @param port
	 * @param objectKeyName
	 * @param compressedFilepath
	 * @throws IOException
	 */
	public void downloadSSEFileFromS3(String accessKey, String secretKey, String bucketName, String objectKeyName,
			String objectKey, String compressedFilepath, String proxy, Integer port) throws IOException {
		try {
		// Construct an instance of AmazonS3Client
		AmazonS3 s3Obj;
		BasicAWSCredentials awsCreds;
		// Construct an instance of AmazonS3
		if (accessKey.equalsIgnoreCase("NA") && secretKey.equalsIgnoreCase("NA")) {
			s3Obj = AmazonS3ClientBuilder.standard().withCredentials(InstanceProfileCredentialsProvider.getInstance())
					.build();
		} else {
			awsCreds = new BasicAWSCredentials(accessKey, secretKey);
			ClientConfiguration cc = new ClientConfiguration();
			if ((proxy != null) && (port != null) || ((proxy != null) && (port != 0))) {
				cc.setProxyHost(proxy);
				cc.setProxyPort(port);
				s3Obj = new AmazonS3Client(awsCreds, cc);
			} else {
				s3Obj = new AmazonS3Client(awsCreds);
			}
		}
		// set SSE to decrypt
		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

	
			File compressedFile = (new File(compressedFilepath));
			GetObjectRequest request = new GetObjectRequest(bucketName, objectKey);
			s3Obj.getObject(request, compressedFile);
			System.out.println("Your compressed file from S3----" + compressedFilepath);

		} catch (AmazonClientException ace) {
			ace.printStackTrace();
			System.out.println("Exception: " + ace);
			System.out.println(
					"Caught an AmazonClientException in downloading File, which means the client encountered an internal error while trying to communicate with S3");
			System.out.println("Error Message: " + ace.getMessage());
		}

	}

	/**
	 *  Method to delete file from S3
	 *  
	 * @param bucketName
	 * @param objectKeyName
	 * @param fileName
	 * @param AccessKeyId
	 * @param SecretKey
	 * @param proxy
	 * @param port
	 */
	public void deleteFileFromS3(String bucketName, String objectKeyName, String fileName, String AccessKeyId,
			String SecretKey, String proxy, Integer port) {

		AmazonS3 s3Obj;
		// Construct an instance of AmazonS3
		try {
			if ((AccessKeyId.equalsIgnoreCase("NA") && SecretKey.equalsIgnoreCase("NA"))
					|| ((AccessKeyId == null) && (SecretKey == null))) {
				s3Obj = AmazonS3ClientBuilder.standard()
						.withCredentials(InstanceProfileCredentialsProvider.getInstance()).build();
			} else {
				BasicAWSCredentials awsCreds = new BasicAWSCredentials(AccessKeyId, SecretKey);
				ClientConfiguration cc = new ClientConfiguration();
				if ((proxy != null) && (port != null) || ((proxy != null) && (port != 0))) {
					cc.setProxyHost(proxy);
					cc.setProxyPort(port);
					s3Obj = new AmazonS3Client(awsCreds, cc);
				} else {
					s3Obj = new AmazonS3Client(awsCreds);
				}
			}
			if (objectKeyName != null) {
				s3Obj.deleteObject(new DeleteObjectRequest(bucketName, objectKeyName + fileName));
			} else {
				s3Obj.deleteObject(new DeleteObjectRequest(bucketName, fileName));
			}

			System.out.println("Your File " + fileName + " is deleted from bucket " + bucketName);
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException.");
			System.out.println("Exception:    " + ase);
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			ace.printStackTrace();
			System.out.println("Exception:" + ace);
			System.out.println("Error Message: " + ace.getMessage());
			System.out.println("Caught an AmazonClientException in deleting File");
		}

	}
}
