from flask import Flask, request, json, jsonify
import os

app = Flask(__name__,
            static_folder=os.path.join(os.path.dirname(
                os.path.realpath( __file__ )),
                                       "static")
           )

request_store = []

def store_request():
    try:
        data = request.json
        headers = request.headers
        request_store.append(
            {"data": data,
             "headers": dict(request.headers)}
        )
        return 'ok'
    except Exception, e:
        request_store.append(
        str(e)
        )



@app.route("/api/action/resource_update", methods=['GET', 'POST'])
def resource_update():
    return store_request()

@app.route("/api/action/task_status_update", methods=['GET', 'POST'])
def task_update():
    return store_request()

@app.route("/last_request", methods=['GET', 'POST'])
def last_request():
    return jsonify(request_store.pop())

@app.route("/", methods=['GET', 'POST'])
def ok():
    return 'ok'

if __name__ == "__main__":
    app.run(port=50001)
