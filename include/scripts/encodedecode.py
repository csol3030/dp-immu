import base64

file = open('VA Immunizations_0x3BC916D5_SECRET.asc', 'rb')
# file = open('test.asc', 'rb')
file_content = file.read()

# base64_one = base64.encodestring(file_content)
base64_two = base64.b64encode(file_content).decode('ascii')

# print(type(base64_one))
print(base64_two)

# base64_decode = base64.b64decode(base64_two).decode('ascii')
# print(base64_decode)