# Upload a file

In this section, we will upload a file to Edge node.

## Pre-requisites
- make sure you have a edge node running either locally or remote. Use this guide [running a node](running_node.md) to run a node.
- identify the edge node host.
- get a API key using this guide [getting an API key](getting-api-key.md)

## Upload a file
Once you have a node and API key, you can upload a file to the node using the following command:
```bash
curl --location 'http://localhost:1313/api/v1/content/add' \
--header 'Authorization: Bearer [API_KEY]' \
--form 'data=@"/path/to/file"'
--tag_name='mytag1'
{
    "status": "success",
    "message": "File uploaded and pinned successfully. Please take note of the ids.",
    "contents": [
        {
            "CreatedAt": "0001-01-01T00:00:00Z",
            "UpdatedAt": "0001-01-01T00:00:00Z",
            "DeletedAt": null,
            "ID": 21,
            "name": "https___3038135290-files.gitbook.io_~_files_v0_b_gitbook-x-prod.appspot.com_o_spaces%2F8Ohv82aEc0JVuEXixqN2%2Flogo%2F1ed4UmhvUsIHrDTNYr0v%2FFDT%20Logo_1%404x-8 (1).png",
            "size": 5114,
            "cid": "bafybeicxagr5utxtgndszbmfe5i3lxq2bkuzb4fgwyw57zzvaz6gyb5igm",
            "bucket_uuid": "561be458-1538-11ee-bb54-9e0bf0c70138",
            "status": "pinned",
            "make_deal": true,
            "tag_name": "mytag1",
            "created_at": "2023-06-27T18:17:00.986323-04:00",
            "updated_at": "2023-06-27T18:17:00.986324-04:00"
        }
    ]
}
```

## View the file using the gateway url
```
http://localhost:1313/gw/<cid>
http://localhost:1313/gw/content/<content_id>
```
