from fastapi import FastAPI, status, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import json
from pydantic import BaseModel
from datetime import datetime
from confluent_kafka import Producer

# Create class (schema) for the JSON
# Date get's ingested as string and then before writing validated
class InvoiceItem(BaseModel):
    InvoiceNo: int
    StockCode: str
    Description: str
    Quantity: int
    InvoiceDate: str
    UnitPrice: float
    CustomerID: int
    Country: str

# This is important for general execution and the docker later
app = FastAPI()

# Base URL
@app.get("/")
async def root():
    return {"message": "Hello World"}

# Add a new invoice
@app.post("/invoiceitem")
async def post_invoice_item(item: InvoiceItem): #body awaits a json with invoice item information
    print("Message received")
    try:
        # Evaluate the timestamp and parse it to datetime object you can work with
        date = datetime.strptime(item.InvoiceDate, "%d/%m/%Y %H:%M")

        print('Found a timestamp: ', date)

        # Replace strange date with new datetime
        # Use strftime to parse the string in the right format (replace / with - and add seconds)
        item.InvoiceDate = date.strftime("%d-%m-%Y %H:%M:%S")
        print("New item date:", item.InvoiceDate)
        
        # Parse item back to json
        json_of_item = jsonable_encoder(item)
        
        # Dump the json out as string
        json_as_string = json.dumps(json_of_item)
        print(json_as_string)
        
        # Produce the string
        #produce_kafka_string(json_as_string)

        # Encode the created customer item if successful into a JSON and return it to the client with 201
        return JSONResponse(content=json_of_item, status_code=201)
    
    # Will be thrown by datetime if the date does not fit
    # All other value errors are automatically taken care of because of the InvoiceItem Class
    except ValueError:
        return JSONResponse(content=jsonable_encoder(item), status_code=400)
        

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def produce_kafka_string(json_as_string):
    # Configuration for Kafka Producer
    conf = {
        'bootstrap.servers': 'kafka:9092',
        'acks': 'all'
    }

    producer = Producer(conf)

    try:
        # Produce the message and set the delivery report callback.
        # The message will be delivered asynchronously.
        producer.produce('ingestion-topic', json_as_string, callback=delivery_report)
        
        # Wait for any outstanding messages to be delivered and delivery reports to be received.
        producer.flush()
    except Exception as e:
        print(f"Failed to produce message: {e}")