from awsUtils.SecretManager import get_secret


def test_get_secret():
    secret_name = "dev-redshift"
    response = get_secret(secret_name)
    print("username : " + response["username"])
    print("password : " + response["password"])
