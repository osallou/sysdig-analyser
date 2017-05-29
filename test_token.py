'''
Generate a token to test ui

'''

import jwt
import datetime
import sys

container = None
if len(sys.argv) != 2:
    print('Missing container id in arguments')
    sys.exit(1)
container = sys.argv[1]
token = jwt.encode({'container': container,
                    'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=3600*24*2),
                    'aud': 'urn:cb/stat'}, 'mytoken')

print(str(token))
