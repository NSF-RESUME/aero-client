from dsaas_client.api import get_file, ClientError, register_function, create_source
# from osprey.server.lib.globus_compute import register_function

# s_id = input('Enter a source_id : ')
# prompt = input('Enter (y) for specific version or (n) for latest version : ')
# if prompt == 'y':
#     v_id = input('Enter a version_id : ')
# else:
#     v_id = None


def temp(*args, **kwargs):
    kwargs["executed"] = True
    return args, kwargs


try:
    response = register_function(temp)
    assert response.get("code") == 200
    verifier_id = response.get("function_id")
except Exception as e:
    print(e)

source = create_source(
    name="Example - 1",
    url="https://data.cityofchicago.org/resource/x74m-smqb.csv",
    timer=86400,
    description="First of its kind",
    verifier=verifier_id,
)
try:
    file = get_file(
        source["id"]
    )  # source_id for the Source, maybe we can add another api which takes source name and give id
except ClientError as e:
    print(e)
    exit()

print(file.head(5))
