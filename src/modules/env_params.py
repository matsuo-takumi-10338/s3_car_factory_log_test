from databricks.sdk.runtime import dbutils
import os

class EnvParams:
    
    def __init__(self, domain, layer):
        self.layer = layer

        current_path = os.getcwd()
        if "_prod" in current_path:
            self.env = "prod"
        else:
            self.env = "dev"

        if domain == "mom_factory":
            self.domain = "mom_factory"
        else:
            self.domain = "engineering"

        scope_name = f"cf_{self.domain}_secrets_{self.env}"

        self.s3_bucket = dbutils.secrets.get(scope=scope_name, key="s3_bucket")
        self.project_dir = dbutils.secrets.get(scope = scope_name, key = "project_dir")

    def get_path(self, param):

        if param == "s3_source_path":
            if self.domain == "mom_factory":
                return f"s3://{self.s3_bucket}/{self.project_dir}/raw/"
            else:
                return f"s3://{self.s3_bucket}/{self.project_dir}/raw/{self.layer}/"
        
        if param == "schema_location":
            return f"s3://{self.s3_bucket}/{self.project_dir}/checkpoints/{self.layer}/_schema_metadata"
        
        if param == "checkpoint_location":
            return f"s3://{self.s3_bucket}/{self.project_dir}/checkpoints/{self.layer}/"    

        return None
    
    def get_secret_params(self, param):
        
        if param == "s3_bucket":
            return self.s3_bucket
        
        if param == "project_dir":
            return self.project_dir