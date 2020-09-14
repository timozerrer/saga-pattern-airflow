from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit
import requests
import random

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)


@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/booking')
def booking():
    requests.post("http://airflow:8080/api/experimental/dags/saga_workflow/dag_runs",json={})
    return ""


@app.route('/log')
def log():
    text = request.args.get("text")  # Get transaction ID
    socketio.emit("logs",text)
    return ""

if __name__ == '__main__':
    socketio.run(app)