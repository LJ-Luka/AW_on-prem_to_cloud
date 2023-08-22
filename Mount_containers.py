cmd1
#to create mount points for the bronze container. It is named test here because the container was created as part of a tutorial

configs = {
   "fs.adl.oauth2.access.token.provider.type": "CustomAccessTokenProvider",
   "fs.adl.oauth2.access.token.custom.provider": spark.conf.get("spark.databricks.passthrough.adls.tokenProviderClassName")
   }

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
   source = "wasbs://test@storagedemo.blob.core.windows.net/",
   mount_point = "/mnt/test1",
   extra_configs = {'fs.azure.account.key.storagedemo.blob.core.windows.net':'1Sj2vKXhEG2L3KVUJ+yMYwB+ddDSc5LC0n+1Tqb6vc1a/ECKH9hn/ehJsAYuRoGceOfy7N2PbuSX+ASt5EAUug=='})



cmd2
#to view folder (SalesLT within the container) and files within

dbutils.fs.ls("/mnt/test1/SalesLT")





#The same processes in cmd1 and cmd2 will be used to create mount points for the silver and gold containers with some edits (changing container names)
#In the community data bricks version we get an error saying file not found. Perhaps the containers need to be created first, unlike when using the premium db
#After creating new silver and gold containers.


cmd3

configs = {
   "fs.adl.oauth2.access.token.provider.type": "CustomAccessTokenProvider",
   "fs.adl.oauth2.access.token.custom.provider": spark.conf.get("spark.databricks.passthrough.adls.tokenProviderClassName")
   }

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
   source = "wasbs://silver@storagedemo.blob.core.windows.net/",
   mount_point = "/mnt/silver",
   extra_configs = {'fs.azure.account.key.storagedemo.blob.core.windows.net':'1Sj2vKXhEG2L3KVUJ+yMYwB+ddDSc5LC0n+1Tqb6vc1a/ECKH9hn/ehJsAYuRoGceOfy7N2PbuSX+ASt5EAUug=='})


cmd4

configs = {
   "fs.adl.oauth2.access.token.provider.type": "CustomAccessTokenProvider",
   "fs.adl.oauth2.access.token.custom.provider": spark.conf.get("spark.databricks.passthrough.adls.tokenProviderClassName")
   }

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
   source = "wasbs://gold@storagedemo.blob.core.windows.net/",
   mount_point = "/mnt/gold",
   extra_configs = {'fs.azure.account.key.storagedemo.blob.core.windows.net':'1Sj2vKXhEG2L3KVUJ+yMYwB+ddDSc5LC0n+1Tqb6vc1a/ECKH9hn/ehJsAYuRoGceOfy7N2PbuSX+ASt5EAUug=='})