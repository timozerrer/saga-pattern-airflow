from flask import Flask, request
from random import random
import requests
import os
app = Flask(__name__)


@app.route("/")
def local_transaction():
    #Triggering a local transaction
    tid = request.args.get("tid") #Get transaction ID
    rand = random() # 0.0 <= rand < 1.0
    if (rand > 0.7): # Fail randomly
        log(tid, "Failed! Returning error to orchestrator.")
        return "", 406 # Not Acceptable Status Code
    log(tid,"Local transaction successful.")
    return "",200 # End successfully


@app.route("/compensate")
def compensating_transaction():
    # Triggering a compensating action
    tid = request.args.get("tid")  # Get transaction ID
    log(tid, "Local transaction successfully compensated.")
    return "",0


def log(tid, text):
    requests.get("http://interface:5000/log",params={"text": '[TX:{}][{}]: {}'.format(tid, os.environ['HOSTNAME'],text)})