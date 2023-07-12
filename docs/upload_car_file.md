# Upload a CAR file

In this section, we will upload a CAR file to Edge node.

## Pre-requisites
- make sure you have a edge node running either locally or remote. Use this guide [running a node](running_node.md) to run a node.
- identify the edgeurid node host.
- get a API key using this guide [getting an API key](getting-api-key.md)
- CAR file should be valid and can be read by [`go-car`](https://github.com/ipld/go-car)

## Upload a file with a collection name
Once you have a node and API key, you can upload a file to the node using the following command:
```bash
curl --location 'http://localhost:1313/api/v1/content/add-car' \
--header 'Authorization: Bearer [API_KEY]' \
--form 'data=@"/path/to/file.car"'
--collection_name='mycars'
{
    "status": "success",
    "message": "File uploaded and pinned successfully. Please take note of the ids.",
    "contents": [
        {
            "DeletedAt": null,
            "ID": 21,
            "name": "mycar.car",
            "size": 5114,
            "cid": "bafybeicxagr5utxtgndszbmfe5i3lxq2bkuzb4fgwyw57zzvaz6gyb5igm",
            "bucket_uuid": "561be458-1538-11ee-bb54-9e0bf0c70138",
            "status": "pinned",
            "make_deal": true,
            "collection_name": "mycars",
            "created_at": "2023-06-27T18:17:00.986323-04:00",
            "updated_at": "2023-06-27T18:17:00.986324-04:00"
        }
    ]
}
```

## Download the car file using the gateway url
```
http://localhost:1313/gw/<cid>
http://localhost:1313/gw/content/<content_id>
```
