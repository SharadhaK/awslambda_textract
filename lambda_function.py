#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import requests
import random
import boto3


def get_kv_relationship(key_map, value_map, block_map):
    kvs = {}
    for (block_id, key_block) in key_map.items():
        value_block = find_value_block(key_block, value_map)
        key = get_text(key_block, block_map)
        val = get_text(value_block, block_map)
        kvs[key] = val
    return kvs


def find_value_block(key_block, value_map):
    for relationship in key_block['Relationships']:
        if relationship['Type'] == 'VALUE':
            for value_id in relationship['Ids']:
                value_block = value_map[value_id]
    return value_block


def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] == 'SELECTED':
                            text += 'X '

    return text


def print_kvs(kvs):
    for (key, value) in kvs.items():
        print (key, ':', value)

def send_scp(kvs,vendorname):
    vendorInvoice = ''
    invoiceDate = ''
    Amount = ''
    dueDate = ''
    url = "https://xxxxx-invoices-srv.cfapps.eu10.hana.ondemand.com/browse/Invoices"
            
    for key in kvs: 
        if key.startswith("Invoice reference:") or key.startswith("Invoice #"):
            vendorInvoice = kvs[key]
        if key.startswith("Issue date") or key.startswith("Invoice Date"):
            invoiceDate = kvs[key]
        if key.startswith("Amount due") or key.startswith("TOTAL") or key.startswith("INVOICE TOTAL"):
            Amount = kvs[key]
            Amount = Amount[1:]
            Currency = "GBP"
        if key.startswith("Payment due by:") or key.startswith("Due Date"):
            dueDate = kvs[key] 
    
    id = random.randint(100, 999)
    iduse = str(id)
    payload =  "{\"id\": "+ iduse +", \"vendorName\":\""+vendorname+"\", \"vendorInvoice\":\""+vendorInvoice+"\", \"invoiceDate\":\""+invoiceDate+"\", \"Amount\":\""+Amount+"\",\"currency\":\""+Currency+"\", \"dueDate\":\""+dueDate+"\"  }"
    print(payload)
    headers = {
     'Content-Type': 'application/json;charset=UTF-8;IEEE754Compatible=true'
    }
    response = requests.request("POST", url, headers=headers, data = payload)
    print(response)
    return response
    
    
def lambda_handler(event, context):
    
    print(event)
    destination_bucket_name = 'processedinvoiceskt'
   # Bucket Name where file was uploaded
    source_bucket_name = event['Records'][0]['s3']['bucket']['name']
   # Filename of object (with path)
    file_key_name = event['Records'][0]['s3']['object']['key']

    client = boto3.client('textract')
    response = client.analyze_document(Document={'S3Object': {'Bucket': source_bucket_name,
                                'Name': file_key_name}},
                                FeatureTypes=['FORMS'])


    blocks = response['Blocks']
    key_map = {}
    value_map = {}
    block_map = {}
    for block in blocks:
      block_id = block['Id']
      block_map[block_id] = block
      if block['BlockType'] == 'KEY_VALUE_SET':
        if 'KEY' in block['EntityTypes']:
            key_map[block_id] = block
        else:
            value_map[block_id] = block

    # Get Key Value relationship

    kvs = get_kv_relationship(key_map, value_map, block_map)
   
   # Call Amazon Textract
    response = client.detect_document_text(
    Document={
        'S3Object': {
            'Bucket': source_bucket_name,
            'Name': file_key_name
        }
    })

    # Copy Source Object
    copy_source_object = {'Bucket': source_bucket_name,
                          'Key': file_key_name}
   # boto3 S3 initialization
    s3_client = boto3.client('s3')
   # S3 copy object operation
    s3_client.copy_object(CopySource=copy_source_object,
                          Bucket=destination_bucket_name,
                          Key=file_key_name)
    print("copied")
    
    # Print detected text
    for item in response["Blocks"]:
        if item["BlockType"] == "LINE":
            print (item["Text"])
            vendorname = item["Text"]
            break
            
    response = send_scp(kvs,vendorname)
    print(response)
   
    s3_client.delete_object(Bucket=source_bucket_name,Key=file_key_name );
    
    print("deleted")
    return {'statusCode': 200,
            'body': json.dumps('file deleted!')}
