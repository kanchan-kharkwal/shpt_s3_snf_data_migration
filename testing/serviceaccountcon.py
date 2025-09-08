import json
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# Eventually this will be replaced by user and creds from SSM
# from config_parameters import Config_parameters
# Abhishek
SF_API_USER_NAME = "SRV_QAVALIDATION@eversana.com"
# SF_API_USER_NAME = "SUDIP.RAY@EVERSANA.COM"

def get_eversana_secure_connection(
    user=SF_API_USER_NAME,
    warehouse="ANALYST_WH",
    # database=Config_parameters.database,
    # schema=Config_parameters.schema,
    role="EDADATAENGINEER",
    account="eversana.us-east-2.aws",
    authentication="authenticator",
    authenticator="externalbrowser",
):
    print("****************in getting secure connection*************")
    # with open("EversanaSFTask_private.p8", "rb") as key:
    with open("EversanaSFTask_private.p8", "rb") as key:
        p_key = serialization.load_pem_private_key(key.read(), password=None, backend=default_backend())
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        con = snowflake.connector.connect(
            user=user,
            account=account,
            private_key=pkb,
            warehouse=warehouse,
            role=role,
            authentication=authentication,
            authenticator=authenticator,
        )
        print("conn")
        return con


# 05-01-2023
def get_eversana_secure_connection_WEST(
    user=SF_API_USER_NAME,
    warehouse="CLIENT_WH", ###warehouse="EDA_ETL_WH" 17 Nov 2023
    # database=Config_parameters.database,
    # schema=Config_parameters.schema,
    role="EDAQA",
    account="eversana-eversana_aws_uswest2",
    # account="eversana_aws_uswest2",
    authentication="authenticator",
    authenticator="externalbrowser",
):
    print("****************in getting secure connection*************")
    with open("EversanaSFTask_private.p8", "rb") as key:
        p_key = serialization.load_pem_private_key(key.read(), password=None, backend=default_backend())
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        con = snowflake.connector.connect(
            user=user,
            account=account,
            private_key=pkb,
            warehouse=warehouse,
            role=role,
            authentication=authentication,
            authenticator=authenticator,
        )
        print("conn")
        return con
