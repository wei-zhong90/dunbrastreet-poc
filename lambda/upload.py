import json

def handler(event, context):
  print("request:", json.dumps(event))
  
  response = {
    'statusCode': 200,
    'headers': { "Content-Type": "text/plain" },
    'body': f"Hello, CDK! You've hit {event['path']}\n"
  }
  return response