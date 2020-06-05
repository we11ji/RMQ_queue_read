from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator
import couchbase.subdocument as SD
import json
import base64
import gzip
import io

cluster = Cluster('couchbase://localhost')
authenticator = PasswordAuthenticator('Administrator', 'Administrator')
cluster.authenticate(authenticator)

def decompress(data):
	#unzip byte to str
    with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:
        return f.read()

def compress(data):
	#zip str to byte
	out = io.BytesIO()
	with gzip.GzipFile(fileobj=out, mode='w') as f:
		f.write(data.encode())
	return out.getvalue()

def iterate(dictionary,key_to_del):
	#deletes in dictionary all occurances of key_to_del
	for key,value in dictionary.items():
		if key == key_to_del:
			del dictionary[key]
			break
		if isinstance(value, dict):
			iterate(value,key_to_del)

#with open('dest.json','w') as dest_file:
#	iterate(json_obj,'attributes')
#	dest_file.write(json.dumps(json_obj, indent=2))
def remove_item(bucket_name, doc_id, item):
	while True:		
		cb_bucket = cluster.open_bucket(bucket_name)
		document = cb_bucket.get(doc_id)
		cur_cas = document.cas
		ifEncoded = cb_bucket.lookup_in(doc_id, SD.get('isBase64'))
		ifZiped = cb_bucket.lookup_in(doc_id, SD.get('isZip'))		
		
		if ifZiped[0] == False and ifEncoded[0] == False:
			#read body
			body = cb_bucket.lookup_in(doc_id, SD.get('body'))[0]
			json_obj = json.loads(body)
			#print(json.dumps(json_obj, indent=2))			
			iterate(json_obj,item)
			try:
				cb_bucket.mutate_in(doc_id, SD.replace('body', json.dumps(json_obj)),cas = cur_cas)
				break
			except KeyExistsError:
				continue
		else:
			bytes_body = decompress(base64.b64decode(cb_bucket.lookup_in(doc_id, SD.get('body'))[0]))
			json_obj = json.loads(bytes_body)
			print(json.dumps(json_obj, indent=2))			
			iterate(json_obj,item)
			json_obj = base64.b64encode(compress(json.dumps(json_obj))).decode('utf-8')
			try:
				cb_bucket.mutate_in(doc_id, SD.replace('body', json.dumps(json_obj)[1:-1]),cas = cur_cas)
				break
			except KeyExistsError:
				continue


with open("substodel.txt", "r") as read_file:
	for subId in read_file:
		item = f'SUB11_{subId}'.strip('\n\r')
		remove_item('bucket1', 'sub2', item)