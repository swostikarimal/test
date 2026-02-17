# Databricks notebook source
# Example: Azure ADLS Gen2 mount
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "<your-client-id>",
    "fs.azure.account.oauth2.client.secret": "<your-client-secret>",
    "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<your-tenant-id>/oauth2/token"
}

# Mount points
mounts = {
    "/mnt/bronze": "abfss://<container>@<storage-account>.dfs.core.windows.net/bronze",
    "/mnt/silver": "abfss://<container>@<storage-account>.dfs.core.windows.net/silver",
    "/mnt/gold": "abfss://<container>@<storage-account>.dfs.core.windows.net/gold"
}

for mount_point, source in mounts.items():
    try:
        dbutils.fs.mount(source=source, mount_point=mount_point, extra_configs=configs)
        print(f"Mounted {mount_point}")
    except Exception as e:
        print(f"{mount_point} may already be mounted: {e}")
