import json

json_string = {
    "id": 1,
    "username" : "Lee",
    "email" : "abc@gmail.com",
    "address" : {
        "street" : "ABC",
        "cirt" : "Seoul",
    },
    "details" : ""
}
json_dump = json.dumps(json_string)

print(json_dump)

json_object = json.loads(json_dump)


with open('json_string.json') as f: #file path or file
    json_data = json.load(f)
    print(json_data)

    id = json_data["id"]
    username = json_data["username"]
    email = json_data["email"]

    dump = (id, username, email)
    print(dump)

    dump2 = {"id": id, "username" :username, "email" : email}
    print(dump2)


path = "./json_string2.json" 
with open(path, 'w', encoding='utf-8') as file:
    json.dump(dump2, file, indent="\t")