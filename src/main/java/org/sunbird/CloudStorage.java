package org.sunbird;


import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.cloud.storage.BaseStorageService;
import org.sunbird.cloud.storage.factory.StorageConfig;
import org.sunbird.cloud.storage.factory.StorageServiceFactory;
import scala.Option;

import java.io.File;


public class CloudStorage {

    private Logger logger = LoggerFactory.getLogger(CloudStorage.class);

    private static BaseStorageService storageService = null;

    private static String cloudStoreType = System.getenv("cert_cloud_storage_type");

    private static String containerName = System.getenv("CONTAINER_NAME");

    static {
        if (StringUtils.equalsIgnoreCase(cloudStoreType, "azure")) {
            String storageKey = System.getenv("cert_azure_storage_key");
            String storageSecret = System.getenv("cert_azure_storage_secret");
            storageService = StorageServiceFactory.getStorageService(new StorageConfig(cloudStoreType, storageKey, storageSecret));
        } else if (StringUtils.equalsIgnoreCase(cloudStoreType, "aws")) {
            String storageKey = System.getenv("cert_aws_storage_key");
            String storageSecret = System.getenv("cert_aws_storage_secret");
            storageService = StorageServiceFactory.getStorageService(new StorageConfig(cloudStoreType, storageKey, storageSecret));
        } else try {
            throw new Exception("ERR_INVALID_CLOUD_STORAGE Error while initialising cloud storage");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getContainerName() {
        return System.getenv("cert_azure_storage_key");
    }

    public static String uploadFile(String path, File file) {
        String objectKey = path + file.getName();
        String url = storageService.upload(containerName,
                file.getAbsolutePath(),
                objectKey,
                Option.apply(false),
                Option.apply(1),
                Option.apply(5), Option.apply(1));
        return UrlManager.getSharableUrl(url, containerName);
    }


    public static void closeConnection() {
        storageService.closeContext();
    }
}
