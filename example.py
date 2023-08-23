from client import source_file, ClientError

s_id = input('Enter a source_id : ')
prompt = input('Enter (y) for specific version or (n) for latest version : ')
if prompt == 'y':
    v_id = input('Enter a version_id : ')
else:
    v_id = None

try:
    file = source_file(s_id, v_id) # source_id for the Source, maybe we can add another api which takes source name and give id
except ClientError as e:
    print(e)
    exit()

print(file.head(5))